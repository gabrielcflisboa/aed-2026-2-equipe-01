package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.EventoRetidoVO;
import br.pucminas.aed.ingressos.domain.PedidoDeReprocessamentoVO;
import br.pucminas.aed.ingressos.domain.ResultadoDoReprocessamentoVO;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class ReprocessamentoService {

    private static final Logger logger = LoggerFactory.getLogger(ReprocessamentoService.class);

    private static final Duration LIMITE_DA_LEITURA = Duration.ofSeconds(10);
    private static final Duration LIMITE_DO_ENVIO = Duration.ofSeconds(10);

    private final ConsumerFactory<String, String> fabricaDeConsumidores;
    private final KafkaTemplate<String, String> clienteDoBroker;
    private final Set<String> dlqsReprocessaveis;
    private final String sufixoDlq;

    public ReprocessamentoService(ConsumerFactory<String, String> fabricaDeConsumidores,
            KafkaTemplate<String, String> clienteDoBroker,
            @Value("${app.resiliencia.dlqs-reprocessaveis}") Set<String> dlqsReprocessaveis,
            @Value("${app.resiliencia.sufixo-dlq}") String sufixoDlq) {
        this.fabricaDeConsumidores = fabricaDeConsumidores;
        this.clienteDoBroker = clienteDoBroker;
        this.dlqsReprocessaveis = Set.copyOf(dlqsReprocessaveis);
        this.sufixoDlq = sufixoDlq;
    }

    public List<EventoRetidoVO> listar(String topicoDlq) {
        exigirConhecida(topicoDlq);
        return ler(topicoDlq).stream().map(ReprocessamentoService::retido).toList();
    }

    public ResultadoDoReprocessamentoVO reprocessar(PedidoDeReprocessamentoVO pedido) {
        exigirConhecida(pedido.getTopicoDlq());
        if (pedido.getCeIds().isEmpty()) {
            throw new IllegalArgumentException("informe ao menos um ce_id");
        }
        var pedidos = new LinkedHashSet<>(pedido.getCeIds());
        var republicados = new ArrayList<String>();
        for (var registro : ler(pedido.getTopicoDlq())) {
            var ceId = texto(registro, "ce_id");
            if (ceId != null && pedidos.contains(ceId) && !republicados.contains(ceId)) {
                enviar(paraOTopicoOriginal(registro));
                republicados.add(ceId);
                logger.info("reprocessado: ce_id={} de {}/{}/{}", ceId,
                        registro.topic(), registro.partition(), registro.offset());
            }
        }
        var naoEncontrados = pedidos.stream().filter(ceId -> !republicados.contains(ceId)).toList();
        return new ResultadoDoReprocessamentoVO(republicados, naoEncontrados);
    }

    private void exigirConhecida(String topicoDlq) {
        if (!dlqsReprocessaveis.contains(topicoDlq)) {
            throw new IllegalArgumentException("topico de DLQ desconhecido: " + topicoDlq);
        }
    }

    private List<ConsumerRecord<String, String>> ler(String topicoDlq) {
        try (Consumer<String, String> leitor =
                fabricaDeConsumidores.createConsumer("servico-ingressos-inspecao-dlq", "inspecao")) {
            var particoes = leitor.partitionsFor(topicoDlq).stream()
                    .map(info -> new TopicPartition(topicoDlq, info.partition()))
                    .toList();
            leitor.assign(particoes);
            leitor.seekToBeginning(particoes);
            var fim = leitor.endOffsets(particoes);
            var lidos = new ArrayList<ConsumerRecord<String, String>>();
            var prazo = Instant.now().plus(LIMITE_DA_LEITURA);
            while (!chegouAoFim(leitor, fim) && Instant.now().isBefore(prazo)) {
                leitor.poll(Duration.ofMillis(500)).forEach(lidos::add);
            }
            return lidos;
        }
    }

    private static boolean chegouAoFim(Consumer<?, ?> leitor, Map<TopicPartition, Long> fim) {
        return fim.entrySet().stream().allMatch(entrada -> leitor.position(entrada.getKey()) >= entrada.getValue());
    }

    private ProducerRecord<String, String> paraOTopicoOriginal(ConsumerRecord<String, String> registro) {
        var destino = texto(registro, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        if (destino == null) {
            destino = registro.topic().substring(0, registro.topic().length() - sufixoDlq.length());
        }
        var cabecalhos = new RecordHeaders();
        for (Header cabecalho : registro.headers()) {
            if (!cabecalho.key().startsWith("kafka_dlt-") && !cabecalho.key().startsWith("dlq_")) {
                cabecalhos.add(cabecalho);
            }
        }
        cabecalhos.add("reprocessado_em", Instant.now().toString().getBytes(StandardCharsets.UTF_8));
        cabecalhos.add("reprocessado_de", "%s/%d/%d".formatted(registro.topic(), registro.partition(),
                registro.offset()).getBytes(StandardCharsets.UTF_8));
        return new ProducerRecord<>(destino, null, registro.key(), registro.value(), cabecalhos);
    }

    private void enviar(ProducerRecord<String, String> registro) {
        try {
            clienteDoBroker.send(registro).get(LIMITE_DO_ENVIO.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrompido) {
            Thread.currentThread().interrupt();
            throw new KafkaException("reprocessamento interrompido", interrompido);
        } catch (ExecutionException | TimeoutException semConfirmacao) {
            throw new KafkaException("o broker nao confirmou o reprocessamento", semConfirmacao);
        }
    }

    private static EventoRetidoVO retido(ConsumerRecord<String, String> registro) {
        return new EventoRetidoVO(
                texto(registro, "ce_id"),
                texto(registro, "ce_type"),
                registro.key(),
                texto(registro, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                inteiro(registro, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                longo(registro, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                texto(registro, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP),
                texto(registro, "dlq_excecao"),
                texto(registro, "dlq_mensagem"),
                texto(registro, "dlq_classificacao"),
                texto(registro, "dlq_falhou_em"),
                registro.partition(),
                registro.offset(),
                registro.value());
    }

    private static String texto(ConsumerRecord<?, ?> registro, String nome) {
        var cabecalho = registro.headers().lastHeader(nome);
        return cabecalho == null ? null : new String(cabecalho.value(), StandardCharsets.UTF_8);
    }

    private static Integer inteiro(ConsumerRecord<?, ?> registro, String nome) {
        var cabecalho = registro.headers().lastHeader(nome);
        return cabecalho == null ? null : ByteBuffer.wrap(cabecalho.value()).getInt();
    }

    private static Long longo(ConsumerRecord<?, ?> registro, String nome) {
        var cabecalho = registro.headers().lastHeader(nome);
        return cabecalho == null ? null : ByteBuffer.wrap(cabecalho.value()).getLong();
    }
}
