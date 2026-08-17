package br.pucminas.aed.vendas.service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
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

@Service
public class VendaCallbackService {

    private static final Logger log = LoggerFactory.getLogger(VendaCallbackService.class);

        private final KafkaTemplate<String, Object> clienteDoBroker;
    private final String origemDoEvento;

    public VendaCallbackService(
            KafkaTemplate<String, Object> clienteDoBroker,
            @Value("${app.evento.origem}") String origemDoEvento) {
        this.clienteDoBroker = clienteDoBroker;
        this.origemDoEvento = origemDoEvento;
    }

        public void publicar(String topico, String chaveDeParticao, String eventoId,
            Instant ocorridoEm, String tipoDoEvento, Object evento) {
        ProducerRecord<String, Object> registro =
            new ProducerRecord<>(topico, chaveDeParticao, evento);

        registro.headers()
            .add("ce_specversion", bytes("1.0"))
            .add("ce_id", bytes(eventoId))
                .add("ce_source", bytes(origemDoEvento))
                .add("ce_type", bytes(tipoDoEvento))
            .add("ce_time", bytes(DateTimeFormatter.ISO_INSTANT.format(ocorridoEm)));

        CompletableFuture<SendResult<String, Object>> envio =
                clienteDoBroker.send(registro);

        envio.whenComplete((resultado, falha) -> {
            if (falha != null) {
                log.error("publicacao recusada: eventoId={} topico={} chave={}",
                        eventoId, topico, chaveDeParticao, falha);
                return;
            }
            RecordMetadata metadados = resultado.getRecordMetadata();
            log.info("publicado: eventoId={} topico={} particao={} offset={} chave={}",
                    eventoId, metadados.topic(), metadados.partition(),
                    metadados.offset(), chaveDeParticao);
        });
    }

    private static byte[] bytes(String valor) {
        return valor.getBytes(StandardCharsets.UTF_8);
    }
}
