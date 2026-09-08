package br.pucminas.aed.ingressos;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "app")
public class IngressoConfig {

    private String topico;
    private Map<String, Map<String, Integer>> abertura = new LinkedHashMap<>();

    public String getTopico() {
        return topico;
    }

    public void setTopico(String topico) {
        this.topico = topico;
    }

    public Map<String, Map<String, Integer>> getAbertura() {
        return abertura;
    }

    public void setAbertura(Map<String, Map<String, Integer>> abertura) {
        this.abertura = abertura;
    }
}
