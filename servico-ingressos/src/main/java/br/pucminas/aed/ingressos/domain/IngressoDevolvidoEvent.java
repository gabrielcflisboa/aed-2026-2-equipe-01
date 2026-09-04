package br.pucminas.aed.ingressos.domain;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class IngressoDevolvidoEvent implements EstoqueEvent {

    public static final String TIPO = "IngressoDevolvido";

    private final int quantidade;
    private final String origemEventoId;
    private final String motivo;

    @JsonCreator
    public IngressoDevolvidoEvent(
            @JsonProperty("quantidade") int quantidade,
            @JsonProperty("origemEventoId") String origemEventoId,
            @JsonProperty("motivo") String motivo) {
        if (quantidade <= 0) {
            throw new IllegalArgumentException("quantidade deve ser maior que zero");
        }
        this.quantidade = quantidade;
        this.origemEventoId = Objects.requireNonNull(origemEventoId, "origemEventoId");
        this.motivo = Objects.requireNonNull(motivo, "motivo");
    }

    public int getQuantidade() {
        return quantidade;
    }

    public String getOrigemEventoId() {
        return origemEventoId;
    }

    public String getMotivo() {
        return motivo;
    }

    @Override
    public String tipo() {
        return TIPO;
    }

    @Override
    public boolean equals(Object outro) {
        return outro instanceof IngressoDevolvidoEvent e
                && e.quantidade == quantidade
                && e.origemEventoId.equals(origemEventoId)
                && e.motivo.equals(motivo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(TIPO, quantidade, origemEventoId, motivo);
    }
}
