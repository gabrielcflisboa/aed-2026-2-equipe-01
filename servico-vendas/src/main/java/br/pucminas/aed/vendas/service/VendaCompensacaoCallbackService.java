package br.pucminas.aed.vendas.service;

import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;


import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import br.pucminas.aed.vendas.domain.IngressoReservaCompensadaEvent;

/**
 * Monta o envelope CloudEvents 1.0 em modo binario para o evento de
 * compensacao e envia com chave de particao, espelhando o
 * {@link VendaCallbackService}. Uma classe por tipo de evento, cada
 * KafkaTemplate tipado ao evento exato: nao ha como, por engano, publicar o
 * evento errado atraves deste servico.
 * O retorno do send() nunca e ignorado; a falha e registrada, nao propagada.
 */
@Service
public class VendaCompensacaoCallbackService {

    private static final Logger log = LoggerFactory.getLogger(VendaCompensacaoCallbackService.class);

    private static final String CE_SPECVERSION = "1.0";

    private final KafkaTemplate<String, IngressoReservaCompensadaEvent> clienteDoBroker;
    private final String topico;
    private final String origemDoEvento;
    private final String tipoDoEvento;

    public VendaCompensacaoCallbackService(
            KafkaTemplate<String, IngressoReservaCompensadaEvent> clienteDoBroker,
            @Value("${app.topico-compensacoes}") String topico,
            @Value("${app.evento.origem}") String origemDoEvento,
            @Value("${app.evento.tipo-compensacoes}") String tipoDoEvento) {
        this.clienteDoBroker = clienteDoBroker;
        this.topico = topico;
        this.origemDoEvento = origemDoEvento;
        this.tipoDoEvento = tipoDoEvento;
    }

    /**
     * Publica o fato ja decidido pela VendaService.
     *
     * @param evento          o fato ocorrido, imutavel e com identidade propria
     * @param chaveDeParticao a menor unidade cuja ordem o negocio exige
     */
    public void publicar(IngressoReservaCompensadaEvent evento, String chaveDeParticao) {
        ProducerRecord<String, IngressoReservaCompensadaEvent> registro =
                new ProducerRecord<>(topico, chaveDeParticao, evento);

        registro.headers()
                .add("ce_specversion", bytes(CE_SPECVERSION))
                .add("ce_id", bytes(evento.getEventoId()))
                .add("ce_source", bytes(origemDoEvento))
                .add("ce_type", bytes(tipoDoEvento))
                .add("ce_time", bytes(DateTimeFormatter.ISO_INSTANT.format(evento.getCompensadoEm())));

        CompletableFuture<SendResult<String, IngressoReservaCompensadaEvent>> envio =
                clienteDoBroker.send(registro);

        envio.whenComplete((resultado, falha) -> {
            if (falha != null) {
                log.error("publicacao recusada: eventoId={} topico={} chave={}",
                        evento.getEventoId(), topico, chaveDeParticao, falha);
                return;
            }
            RecordMetadata metadados = resultado.getRecordMetadata();
            log.info("publicado: eventoId={} topico={} particao={} offset={} chave={}",
                    evento.getEventoId(), metadados.topic(), metadados.partition(),
                    metadados.offset(), chaveDeParticao);
        });
    }

    private static byte[] bytes(String valor) {
        return valor.getBytes(StandardCharsets.UTF_8);
    }
}
