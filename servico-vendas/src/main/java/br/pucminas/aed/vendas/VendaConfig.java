package br.pucminas.aed.vendas;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import br.pucminas.aed.vendas.domain.IngressoReservadoEvent;

/**
 * Config do produtor e dos parametros da regra de negocio.
 *
 * O limite por CPF e a capacidade dos setores estão no application.yml, e
 * nao como constante no codigo: sao numeros de negocio, que mudam por
 * evento, e assim da para alterar sem recompilar nada.
 *
 * O formato ISO-8601 da data ja e garantido pelo @JsonFormat no proprio
 * evento (ver IngressoReservadoEvent.getReservadoEm()), sem depender de
 * customizer global do ObjectMapper.
 */
@Configuration
@ConfigurationProperties(prefix = "app")
public class VendaConfig {

    private String topico;
    private int limitePorCpf;
    private Map<String, Integer> setores = new LinkedHashMap<>();

    @Bean
    public KafkaTemplate<String, IngressoReservadoEvent> kafkaTemplate(
            ProducerFactory<String, IngressoReservadoEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    public String getTopico() {
        return topico;
    }

    public void setTopico(String topico) {
        this.topico = topico;
    }

    public int getLimitePorCpf() {
        return limitePorCpf;
    }

    public void setLimitePorCpf(int limitePorCpf) {
        this.limitePorCpf = limitePorCpf;
    }

    public Map<String, Integer> getSetores() {
        return setores;
    }

    public void setSetores(Map<String, Integer> setores) {
        var normalizados = new LinkedHashMap<String, Integer>();
        setores.forEach((setor, capacidade) -> normalizados.put(normalizar(setor), capacidade));
        this.setores = normalizados;
    }

    public static String normalizar(String setor) {
        return setor == null ? null : setor.trim().toUpperCase(Locale.ROOT);
    }
}
