package br.pucminas.aed.ingressos.controller;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import br.pucminas.aed.ingressos.domain.IngressoLiberadoEvent;
import br.pucminas.aed.ingressos.domain.IngressoReservadoEvent;
import br.pucminas.aed.ingressos.service.IngressoService;
import tools.jackson.databind.ObjectMapper;

@Component
public class IngressoListener {

    private static final Logger logger = LoggerFactory.getLogger(IngressoListener.class);

    private final IngressoService ingressoService;
    private final ObjectMapper conversorJson = new ObjectMapper();

    public IngressoListener(IngressoService ingressoService) {
        this.ingressoService = ingressoService;
    }

    @KafkaListener(topics = "${app.topico}", groupId = "${spring.kafka.consumer.group-id}")
    public void receber(ConsumerRecord<String, String> registro, Acknowledgment ack) {
        var evento = conversorJson.readValue(registro.value(), IngressoReservadoEvent.class);
        logger.info("reserva recebida: ce_id={} evento={} particao={} offset={}",
                ceId(registro), evento.getEvento(), registro.partition(), registro.offset());

        this.ingressoService.processarReserva(evento);

        ack.acknowledge();
        logger.info("reserva processada e confirmada: ce_id={}", ceId(registro));
    }

    @KafkaListener(topics = "${app.topico-liberacoes}", groupId = "${spring.kafka.consumer.group-id}")
    public void receberLiberacao(ConsumerRecord<String, String> registro, Acknowledgment ack) {
        var liberacao = conversorJson.readValue(registro.value(), IngressoLiberadoEvent.class);
        logger.info("liberacao recebida: ce_id={} reservaEventoId={} particao={} offset={}",
                ceId(registro), liberacao.getReservaEventoId(), registro.partition(), registro.offset());

        this.ingressoService.processarLiberacao(liberacao);

        ack.acknowledge();
        logger.info("liberacao processada e confirmada: ce_id={}", ceId(registro));
    }

    private static String ceId(ConsumerRecord<?, ?> registro) {
        var cabecalho = registro.headers().lastHeader("ce_id");
        return cabecalho == null ? null : new String(cabecalho.value(), StandardCharsets.UTF_8);
    }
}