package br.pucminas.aed.ingressos.domain;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class IngressoRetiradoEvent implements EstoqueEvent {

    public static final String TIPO = "IngressoRetirado";

    private final int quantidade;
    private final String origemEventoId;

    @JsonCreator
    public IngressoRetiradoEvent(
            @JsonProperty("quantidade") int quantidade,
            @JsonProperty("origemEventoId") String origemEventoId) {
        if (quantidade <= 0) {
            throw new IllegalArgumentException("quantidade deve ser maior que zero");
        }
        this.quantidade = quantidade;
        this.origemEventoId = Objects.requireNonNull(origemEventoId, "origemEventoId");
    }

    public int getQuantidade() {
        return quantidade;
    }

    public String getOrigemEventoId() {
        return origemEventoId;
    }

    @Override
    public String tipo() {
        return TIPO;
    }

    @Override
    public boolean equals(Object outro) {
        return outro instanceof IngressoRetiradoEvent e
                && e.quantidade == quantidade
                && e.origemEventoId.equals(origemEventoId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(TIPO, quantidade, origemEventoId);
    }
}
