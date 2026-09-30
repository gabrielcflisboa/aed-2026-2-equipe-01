package br.pucminas.aed.ingressos.controller;

import java.nio.charset.StandardCharsets;

import tools.jackson.databind.ObjectMapper;

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
    private final ObjectMapper objectMapper = new ObjectMapper();

    public AgregadorDeReservasListener(AgregacaoDeReservasService agregacaoDeReservasService) {
        this.agregacaoDeReservasService = agregacaoDeReservasService;
    }

    @KafkaListener(topics = "${app.topico-reservas}", groupId = "${app.agregador.group-id}")
    public void receberReserva(ConsumerRecord<String, String> registro, Acknowledgment ack) {
        var evento = objectMapper.readValue(registro.value(), IngressoReservadoEvent.class);

        agregacaoDeReservasService.agregar(evento);

        ack.acknowledge();

        logger.info("agregacao atualizada: ce_id={} evento={} particao={} offset={}",
                ceId(registro), evento.getEvento(), registro.partition(), registro.offset());
    }

    private static String ceId(ConsumerRecord<?, ?> registro) {
        var cabecalho = registro.headers().lastHeader("ce_id");
        return cabecalho == null ? null : new String(cabecalho.value(), StandardCharsets.UTF_8);
    }
}
