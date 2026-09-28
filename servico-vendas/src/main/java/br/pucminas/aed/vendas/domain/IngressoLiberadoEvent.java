package br.pucminas.aed.vendas.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class IngressoLiberadoEvent {

    private final String eventoId;
    private final String compraId;
    private final String reservaEventoId;
    private final String pagamentoEventoId;
    private final String evento;
    private final List<ItemDoIngressoVO> itens;
    private final String motivo;
    private final Instant liberadoEm;

    @JsonCreator
    public IngressoLiberadoEvent(
            @JsonProperty("eventoId") String eventoId,
            @JsonProperty("compraId") String compraId,
            @JsonProperty("reservaEventoId") String reservaEventoId,
            @JsonProperty("pagamentoEventoId") String pagamentoEventoId,
            @JsonProperty("evento") String evento,
            @JsonProperty("itens") List<ItemDoIngressoVO> itens,
            @JsonProperty("motivo") String motivo,
            @JsonProperty("liberadoEm") Instant liberadoEm) {
        this.eventoId = Objects.requireNonNull(eventoId, "eventoId");
        this.compraId = Objects.requireNonNull(compraId, "compraId");
        this.reservaEventoId = Objects.requireNonNull(reservaEventoId, "reservaEventoId");
        this.pagamentoEventoId = Objects.requireNonNull(pagamentoEventoId, "pagamentoEventoId");
        this.evento = Objects.requireNonNull(evento, "evento");
        this.itens = List.copyOf(new ArrayList<>(Objects.requireNonNull(itens, "itens")));
        this.motivo = Objects.requireNonNull(motivo, "motivo");
        this.liberadoEm = Objects.requireNonNull(liberadoEm, "liberadoEm");
    }

    public static IngressoLiberadoEvent novo(IngressoReservadoEvent reserva, PagamentoRecusadoEvent recusa) {
        return new IngressoLiberadoEvent(UUID.randomUUID().toString(), reserva.getCompraId(),
                reserva.getEventoId(), recusa.getEventoId(), reserva.getEvento(), reserva.getItens(),
                recusa.getMotivo(), Instant.now());
    }

    public String getEventoId() {
        return eventoId;
    }

    public String getCompraId() {
        return compraId;
    }

    public String getReservaEventoId() {
        return reservaEventoId;
    }

    public String getPagamentoEventoId() {
        return pagamentoEventoId;
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

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    public Instant getLiberadoEm() {
        return liberadoEm;
    }
}