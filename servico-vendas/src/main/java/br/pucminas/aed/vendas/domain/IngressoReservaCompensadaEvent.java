package br.pucminas.aed.vendas.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class IngressoReservaCompensadaEvent {

    private final String eventoId;
    private final String compraId;
    private final String evento;
    private final List<ItemDoIngressoVO> itens;
    private final Instant compensadoEm;

    @JsonCreator
    public IngressoReservaCompensadaEvent(
            @JsonProperty("eventoId") String eventoId,
            @JsonProperty("compraId") String compraId,
            @JsonProperty("evento") String evento,
            @JsonProperty("itens") List<ItemDoIngressoVO> itens,
            @JsonProperty("compensadoEm") Instant compensadoEm) {
        this.eventoId = Objects.requireNonNull(eventoId, "eventoId");
        this.compraId = Objects.requireNonNull(compraId, "compraId");
        this.evento = Objects.requireNonNull(evento, "evento");
        this.itens = List.copyOf(new ArrayList<>(Objects.requireNonNull(itens, "itens")));
        this.compensadoEm = Objects.requireNonNull(compensadoEm, "compensadoEm");
    }

    public static IngressoReservaCompensadaEvent novo(IngressoReservadoEvent reserva) {
        return new IngressoReservaCompensadaEvent(UUID.randomUUID().toString(), reserva.getCompraId(),
                reserva.getEvento(), reserva.getItens(), Instant.now());
    }

    public String getEventoId() {
        return eventoId;
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

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    public Instant getCompensadoEm() {
        return compensadoEm;
    }
}