package br.pucminas.aed.ingressos.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import br.pucminas.aed.ingressos.domain.IngressoReservadoEvent;
import br.pucminas.aed.ingressos.service.AgregacaoDeReservasService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class AgregadorDeReservasListener {

    private static final Logger logger = LoggerFactory.getLogger(AgregadorDeReservasListener.class);

    private final AgregacaoDeReservasService agregacaoDeReservasService;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public AgregadorDeReservasListener(AgregacaoDeReservasService agregacaoDeReservasService) {
        this.agregacaoDeReservasService = agregacaoDeReservasService;
    }

    @KafkaListener(topics = "${app.topico-reservas}", groupId = "${app.agregador.group-id}")
    public void receberReserva(ConsumerRecord<String, String> registro, Acknowledgment ack)
            throws JsonProcessingException {
        var evento = objectMapper.readValue(registro.value(), IngressoReservadoEvent.class);

        agregacaoDeReservasService.agregar(evento);

        ack.acknowledge();

        logger.info("agregacao atualizada: eventoId={} evento={} particao={} offset={}",
                evento.getEventoId(), evento.getEvento(), registro.partition(), registro.offset());
    }
}
