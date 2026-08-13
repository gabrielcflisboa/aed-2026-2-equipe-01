package br.pucminas.aed.vendas;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import br.pucminas.aed.vendas.domain.IngressoReservadoEvent;

/**
 * Config do produtor: KafkaTemplate tipado no evento.
 *
 * O formato ISO-8601 da data ja e garantido pelo @JsonFormat no proprio
 * evento (ver IngressoReservadoEvent.getReservadoEm()), sem depender de
 * customizer global do ObjectMapper.
 */
@Configuration
public class VendaConfig {

    @Bean
    public KafkaTemplate<String, IngressoReservadoEvent> kafkaTemplate(
            ProducerFactory<String, IngressoReservadoEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
