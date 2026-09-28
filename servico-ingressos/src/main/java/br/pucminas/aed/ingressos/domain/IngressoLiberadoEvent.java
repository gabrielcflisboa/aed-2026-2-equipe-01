package br.pucminas.aed.ingressos.domain;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class IngressoLiberadoEvent {

    private final UUID eventoId;
    private final UUID reservaEventoId;
    private final String compraId;
    private final String evento;
    private final List<ItemDoIngressoVO> itens;
    private final String motivo;
    private final Instant liberadoEm;

    @JsonCreator
    public IngressoLiberadoEvent(
            @JsonProperty("eventoId") UUID eventoId,
            @JsonProperty("reservaEventoId") UUID reservaEventoId,
            @JsonProperty("compraId") String compraId,
            @JsonProperty("evento") String evento,
            @JsonProperty("itens") List<ItemDoIngressoVO> itens,
            @JsonProperty("motivo") String motivo,
            @JsonProperty("liberadoEm") Instant liberadoEm) {
        this.eventoId = Objects.requireNonNull(eventoId, "eventoId");
        this.reservaEventoId = Objects.requireNonNull(reservaEventoId, "reservaEventoId");
        this.compraId = compraId;
        this.evento = Objects.requireNonNull(evento, "evento");
        this.itens = List.copyOf(Objects.requireNonNull(itens, "itens"));
        if (this.itens.isEmpty()) {
            throw new IllegalArgumentException("itens vazio");
        }
        this.motivo = Objects.requireNonNull(motivo, "motivo");
        this.liberadoEm = liberadoEm;
    }

    public UUID getEventoId() {
        return eventoId;
    }

    public UUID getReservaEventoId() {
        return reservaEventoId;
    }

    public String getCompraId() {
        return compraId;
    }

    public String getEvento() {
        return evento;
    }

    public List<ItemDoIngressoVO> getItens() {
        return itens;
    }

    public String getMotivo() {
        return motivo;
    }

    public Instant getLiberadoEm() {
        return liberadoEm;
    }
}