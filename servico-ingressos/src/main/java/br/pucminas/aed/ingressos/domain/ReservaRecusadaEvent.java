package br.pucminas.aed.ingressos.domain;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class ReservaRecusadaEvent implements EstoqueEvent {

    public static final String TIPO = "ReservaRecusada";

    private final int quantidadePedida;
    private final int disponivelNoMomento;
    private final String origemEventoId;

    @JsonCreator
    public ReservaRecusadaEvent(
            @JsonProperty("quantidadePedida") int quantidadePedida,
            @JsonProperty("disponivelNoMomento") int disponivelNoMomento,
            @JsonProperty("origemEventoId") String origemEventoId) {
        this.quantidadePedida = quantidadePedida;
        this.disponivelNoMomento = disponivelNoMomento;
        this.origemEventoId = Objects.requireNonNull(origemEventoId, "origemEventoId");
    }

    public int getQuantidadePedida() {
        return quantidadePedida;
    }

    public int getDisponivelNoMomento() {
        return disponivelNoMomento;
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
        return outro instanceof ReservaRecusadaEvent e
                && e.quantidadePedida == quantidadePedida
                && e.disponivelNoMomento == disponivelNoMomento
                && e.origemEventoId.equals(origemEventoId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(TIPO, quantidadePedida, disponivelNoMomento, origemEventoId);
    }
}
