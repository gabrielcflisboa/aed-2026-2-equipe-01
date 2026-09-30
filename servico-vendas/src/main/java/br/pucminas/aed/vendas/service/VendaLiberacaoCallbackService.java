package br.pucminas.aed.vendas.service;

import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import br.pucminas.aed.vendas.domain.IngressoLiberadoEvent;

@Service
public class VendaLiberacaoCallbackService {

    private static final Logger log = LoggerFactory.getLogger(VendaLiberacaoCallbackService.class);

    private static final String CE_SPECVERSION = "1.0";

    private final KafkaTemplate<String, IngressoLiberadoEvent> clienteDoBroker;
    private final String topico;
    private final String origemDoEvento;
    private final String tipoDoEvento;
    private final long esperaDaConfirmacaoMs;

    public VendaLiberacaoCallbackService(
            KafkaTemplate<String, IngressoLiberadoEvent> clienteDoBroker,
            @Value("${app.topico-liberacoes}") String topico,
            @Value("${app.evento.origem}") String origemDoEvento,
            @Value("${app.evento.tipo-liberacoes}") String tipoDoEvento,
            @Value("${app.resiliencia.espera-da-confirmacao-ms}") long esperaDaConfirmacaoMs) {
        this.clienteDoBroker = clienteDoBroker;
        this.topico = topico;
        this.origemDoEvento = origemDoEvento;
        this.tipoDoEvento = tipoDoEvento;
        this.esperaDaConfirmacaoMs = esperaDaConfirmacaoMs;
    }

    public void publicar(IngressoLiberadoEvent evento, String chaveDeParticao) {
        ProducerRecord<String, IngressoLiberadoEvent> registro =
                new ProducerRecord<>(topico, chaveDeParticao, evento);

        registro.headers()
                .add("ce_specversion", bytes(CE_SPECVERSION))
                .add("ce_id", bytes(evento.getEventoId()))
                .add("ce_source", bytes(origemDoEvento))
                .add("ce_type", bytes(tipoDoEvento))
                .add("ce_time", bytes(DateTimeFormatter.ISO_INSTANT.format(evento.getLiberadoEm())));

        try {
            var metadados = clienteDoBroker.send(registro)
                    .get(esperaDaConfirmacaoMs, TimeUnit.MILLISECONDS)
                    .getRecordMetadata();
            log.info("publicado: eventoId={} reservaEventoId={} topico={} particao={} offset={}",
                    evento.getEventoId(), evento.getReservaEventoId(), metadados.topic(),
                    metadados.partition(), metadados.offset());
        } catch (InterruptedException interrompido) {
            Thread.currentThread().interrupt();
            throw new KafkaException("publicacao da liberacao interrompida: " + evento.getEventoId(), interrompido);
        } catch (ExecutionException | TimeoutException semConfirmacao) {
            log.error("liberacao nao confirmada pelo broker: eventoId={}", evento.getEventoId(), semConfirmacao);
            throw new KafkaException("liberacao nao confirmada pelo broker: " + evento.getEventoId(), semConfirmacao);
        }
    }

    private static byte[] bytes(String valor) {
        return valor.getBytes(StandardCharsets.UTF_8);
    }
}