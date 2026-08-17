package br.pucminas.aed.vendas;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
@ConfigurationProperties(prefix = "app")
public class VendaConfig {

    private String topicoReservas;
    private String topicoCompensacoes;
    private int limitePorCpf;
    private Map<String, Integer> setores = new LinkedHashMap<>();

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    public String getTopicoReservas() {
        return topicoReservas;
    }

    public void setTopicoReservas(String topicoReservas) {
        this.topicoReservas = topicoReservas;
    }

    public String getTopicoCompensacoes() {
        return topicoCompensacoes;
    }

    public void setTopicoCompensacoes(String topicoCompensacoes) {
        this.topicoCompensacoes = topicoCompensacoes;
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
