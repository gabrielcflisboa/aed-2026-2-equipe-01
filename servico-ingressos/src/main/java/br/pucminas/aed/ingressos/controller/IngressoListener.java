package br.pucminas.aed.ingressos.controller;

import tools.jackson.databind.ObjectMapper;

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
    private final ObjectMapper objectMapper = new ObjectMapper();

    public IngressoListener(IngressoService ingressoService) {
        this.ingressoService = ingressoService;
    }

    @KafkaListener(topics = "${app.topico}", groupId = "${spring.kafka.consumer.group-id}")
    public void receber(String mensagem, Acknowledgment ack) {

        // desserializado aqui, nao via value-deserializer: o JacksonJsonDeserializer nao converte em runtime real (ver docs/entregas/aula-05.md)
        var evento = objectMapper.readValue(mensagem, IngressoReservadoEvent.class);

        logger.info("recebido: eventoId={} evento={}", evento.getEventoId(), evento.getEvento());

        this.ingressoService.processarReserva(evento);

        ack.acknowledge();

        logger.info("processado e confirmado: eventoId={}", evento.getEventoId());
    }
}
