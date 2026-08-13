package br.pucminas.aed.vendas.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Fato: uma reserva de ingressos foi aceita para um comprador.
 *
 * Nome no particípio — descreve o que aconteceu, nao um comando. Classe
 * imutavel explicita (sem record, sem setter, copia defensiva da lista) para
 * que os mecanismos fiquem a vista, como pede o padrao da disciplina.
 *
 * eventoId e distinto do id do pedido de compra: dois eventos diferentes
 * podem falar da mesma compra (ex.: reserva e, depois, compensacao), e a
 * chave de deduplicacao no consumidor e sempre o eventoId.
 */
public final class IngressoReservadoEvent {

    private final String eventoId;
    private final String compraId;
    private final String cpfComprador;
    private final String evento;
    private final List<ItemDoIngressoVO> itens;
    private final Instant reservadoEm;

    @JsonCreator
    public IngressoReservadoEvent(
            @JsonProperty("eventoId") String eventoId,
            @JsonProperty("compraId") String compraId,
            @JsonProperty("cpfComprador") String cpfComprador,
            @JsonProperty("evento") String evento,
            @JsonProperty("itens") List<ItemDoIngressoVO> itens,
            @JsonProperty("reservadoEm") Instant reservadoEm) {
        this.eventoId = Objects.requireNonNull(eventoId, "eventoId");
        this.compraId = Objects.requireNonNull(compraId, "compraId");
        this.cpfComprador = Objects.requireNonNull(cpfComprador, "cpfComprador");
        this.evento = Objects.requireNonNull(evento, "evento");
        this.itens = List.copyOf(new ArrayList<>(Objects.requireNonNull(itens, "itens")));
        this.reservadoEm = Objects.requireNonNull(reservadoEm, "reservadoEm");
    }

    public static IngressoReservadoEvent novo(String compraId, String cpfComprador, String evento,
            List<ItemDoIngressoVO> itens) {
        return new IngressoReservadoEvent(UUID.randomUUID().toString(), compraId, cpfComprador,
                evento, itens, Instant.now());
    }

    public String getEventoId() {
        return eventoId;
    }

    public String getCompraId() {
        return compraId;
    }

    public String getCpfComprador() {
        return cpfComprador;
    }

    public String getEvento() {
        return evento;
    }

    public List<ItemDoIngressoVO> getItens() {
        return itens;
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    public Instant getReservadoEm() {
        return reservadoEm;
    }
}
