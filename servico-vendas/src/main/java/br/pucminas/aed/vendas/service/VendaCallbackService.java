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
import java.util.concurrent.CompletableFuture;


import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import br.pucminas.aed.vendas.domain.IngressoReservadoEvent;

/**
 * Monta o envelope CloudEvents 1.0 em modo binario, envia com chave de particao
 * e trata o desfecho do envio.
 * O retorno do send() nunca é ignorado, publicação recusada e a falha mais
 * silenciosa de um produtor Kafka.
 * A falha e registrada, não propagada: quem chamou já respondeu 202 Accepted,
 * e uma exceção lançada dentro de uma callback assincrona nao chegaria ao
 * cliente HTTP de qualquer forma.
 */
@Service
public class VendaCallbackService {

    private static final Logger log = LoggerFactory.getLogger(VendaCallbackService.class);

    private static final String CE_SPECVERSION = "1.0";

    private final KafkaTemplate<String, IngressoReservadoEvent> clienteDoBroker;
    private final String topico;
    private final String origemDoEvento;
    private final String tipoDoEvento;

    public VendaCallbackService(
            KafkaTemplate<String, IngressoReservadoEvent> clienteDoBroker,
            @Value("${app.topico}") String topico,
            @Value("${app.evento.origem}") String origemDoEvento,
            @Value("${app.evento.tipo}") String tipoDoEvento) {
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
    public void publicar(IngressoReservadoEvent evento, String chaveDeParticao) {
        ProducerRecord<String, IngressoReservadoEvent> registro =
                new ProducerRecord<>(topico, chaveDeParticao, evento);

        registro.headers()
                .add("ce_specversion", bytes(CE_SPECVERSION))
                .add("ce_id", bytes(evento.getEventoId()))
                .add("ce_source", bytes(origemDoEvento))
                .add("ce_type", bytes(tipoDoEvento))
                .add("ce_time", bytes(DateTimeFormatter.ISO_INSTANT.format(evento.getReservadoEm())));

        CompletableFuture<SendResult<String, IngressoReservadoEvent>> envio =
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
