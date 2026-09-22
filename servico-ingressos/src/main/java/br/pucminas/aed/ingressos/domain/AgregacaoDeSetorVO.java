package br.pucminas.aed.ingressos.domain;

import java.time.Instant;
import java.util.Objects;

public final class AgregacaoDeSetorVO {

    private final String evento;
    private final String setor;
    private final Instant janelaInicio;
    private final int totalIngressos;

    public AgregacaoDeSetorVO(String evento, String setor, Instant janelaInicio, int totalIngressos) {
        this.evento = Objects.requireNonNull(evento, "evento");
        this.setor = Objects.requireNonNull(setor, "setor");
        this.janelaInicio = Objects.requireNonNull(janelaInicio, "janelaInicio");
        this.totalIngressos = totalIngressos;
    }

    public String getEvento() {
        return evento;
    }

    public String getSetor() {
        return setor;
    }

    public Instant getJanelaInicio() {
        return janelaInicio;
    }

    public int getTotalIngressos() {
        return totalIngressos;
    }
}
