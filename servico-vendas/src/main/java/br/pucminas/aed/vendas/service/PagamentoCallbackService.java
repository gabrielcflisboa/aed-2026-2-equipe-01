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

import br.pucminas.aed.vendas.domain.PagamentoRecusadoEvent;

@Service
public class PagamentoCallbackService {

    private static final Logger log = LoggerFactory.getLogger(PagamentoCallbackService.class);

    private static final String CE_SPECVERSION = "1.0";

    private final KafkaTemplate<String, PagamentoRecusadoEvent> clienteDoBroker;
    private final String topico;
    private final String origemDoEvento;
    private final String tipoDoEvento;

    public PagamentoCallbackService(
            KafkaTemplate<String, PagamentoRecusadoEvent> clienteDoBroker,
            @Value("${app.topico-pagamentos-recusados}") String topico,
            @Value("${app.gateway.origem}") String origemDoEvento,
            @Value("${app.gateway.tipo-pagamentos-recusados}") String tipoDoEvento) {
        this.clienteDoBroker = clienteDoBroker;
        this.topico = topico;
        this.origemDoEvento = origemDoEvento;
        this.tipoDoEvento = tipoDoEvento;
    }

    public void publicar(PagamentoRecusadoEvent evento) {
        ProducerRecord<String, PagamentoRecusadoEvent> registro =
                new ProducerRecord<>(topico, evento.getCompraId(), evento);

        registro.headers()
                .add("ce_specversion", bytes(CE_SPECVERSION))
                .add("ce_id", bytes(evento.getEventoId()))
                .add("ce_source", bytes(origemDoEvento))
                .add("ce_type", bytes(tipoDoEvento))
                .add("ce_time", bytes(DateTimeFormatter.ISO_INSTANT.format(evento.getRecusadoEm())));

        CompletableFuture<SendResult<String, PagamentoRecusadoEvent>> envio = clienteDoBroker.send(registro);

        envio.whenComplete((resultado, falha) -> {
            if (falha != null) {
                log.error("recusa nao publicada: eventoId={} compraId={}",
                        evento.getEventoId(), evento.getCompraId(), falha);
                return;
            }
            RecordMetadata metadados = resultado.getRecordMetadata();
            log.info("recusa publicada: eventoId={} compraId={} topico={} particao={} offset={}",
                    evento.getEventoId(), evento.getCompraId(), metadados.topic(),
                    metadados.partition(), metadados.offset());
        });
    }

    private static byte[] bytes(String valor) {
        return valor.getBytes(StandardCharsets.UTF_8);
    }
}