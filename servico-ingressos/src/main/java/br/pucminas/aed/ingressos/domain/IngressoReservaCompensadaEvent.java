package br.pucminas.aed.ingressos.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class IngressoReservaCompensadaEvent {

    private final UUID eventoId;
    private final String evento;
    private final List<ItemDoIngressoVO> itens;
    private final String motivo;
    private final Instant compensadoEm;

    @JsonCreator
    public IngressoReservaCompensadaEvent(
            @JsonProperty("eventoId") UUID eventoId,
            @JsonProperty("evento") String evento,
            @JsonProperty("itens") List<ItemDoIngressoVO> itens,
            @JsonProperty("motivo") String motivo,
            @JsonProperty("compensadoEm") Instant compensadoEm) {

        this.eventoId = eventoId;
        this.evento = evento;
        this.itens = List.copyOf(itens);
        this.motivo = motivo;
        this.compensadoEm = compensadoEm;
    }

    public UUID getEventoId() {
        return eventoId;
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

    public Instant getCompensadoEm() {
        return compensadoEm;
    }
}
