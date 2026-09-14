package br.pucminas.aed.ingressos.domain;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class SetorAbertoEvent implements EstoqueEvent {

    public static final String TIPO = "SetorAberto";

    private final int capacidade;

    @JsonCreator
    public SetorAbertoEvent(@JsonProperty("capacidade") int capacidade) {
        if (capacidade < 0) {
            throw new IllegalArgumentException("capacidade nao pode ser negativa");
        }
        this.capacidade = capacidade;
    }

    public int getCapacidade() {
        return capacidade;
    }

    @Override
    public String tipo() {
        return TIPO;
    }

    @Override
    public boolean equals(Object outro) {
        return outro instanceof SetorAbertoEvent e && e.capacidade == capacidade;
    }

    @Override
    public int hashCode() {
        return Objects.hash(TIPO, capacidade);
    }
}
