package br.pucminas.aed.ingressos.controller;

import br.pucminas.aed.ingressos.domain.IngressoReservadoEvent;
import br.pucminas.aed.ingressos.service.IngressoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class IngressoListener {

    private static final Logger logger = LoggerFactory.getLogger(IngressoListener.class);

    private final IngressoService ingressoService;

    public IngressoListener(IngressoService ingressoService) {
        this.ingressoService = ingressoService;
    }

    @KafkaListener(topics = "${spring.kafka.consumer.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void receber(
            IngressoReservadoEvent evento,
            Acknowledgment ack) {

        logger.info(
                "Recebendo evento de ingresso reservado. eventoId={}",
                evento.getEventoId());

        ingressoService.processarReserva(evento);

        ack.acknowledge();

        logger.info(
                "Evento de ingresso reservado processado. eventoId={}",
                evento.getEventoId());
    }
}