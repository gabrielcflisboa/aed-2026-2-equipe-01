package br.pucminas.aed.ingressos.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import br.pucminas.aed.ingressos.domain.IngressoReservaCompensadaEvent;
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
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public IngressoListener(IngressoService ingressoService) {
        this.ingressoService = ingressoService;
    }

    @KafkaListener(topics = "${app.topico-reservas}")
    public void receberReserva(String mensagem, Acknowledgment ack) throws JsonProcessingException {
        var evento = objectMapper.readValue(mensagem, IngressoReservadoEvent.class);

        logger.info(
                "Recebendo evento de ingresso reservado. eventoId={}",
                evento.getEventoId());

        ingressoService.processarReserva(evento);

        ack.acknowledge();

        logger.info(
                "Evento de ingresso reservado processado. eventoId={}",
                evento.getEventoId());
    }

    @KafkaListener(topics = "${app.topico-compensacoes}")
    public void receberCompensacao(String mensagem, Acknowledgment ack) throws JsonProcessingException {
        var evento = objectMapper.readValue(mensagem, IngressoReservaCompensadaEvent.class);

        logger.info("Recebendo evento de compensacao. eventoId={}", evento.getEventoId());
        ingressoService.processarCompensacao(evento);
        ack.acknowledge();
        logger.info("Evento de compensacao processado. eventoId={}", evento.getEventoId());
    }
}