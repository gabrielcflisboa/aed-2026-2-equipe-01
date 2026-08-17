package br.pucminas.aed.ingressos.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class IngressoReservadoEvent {

    private final UUID eventoId;
    private final String evento;
    private final List<ItemDoIngressoVO> itens;

    @JsonCreator
    public IngressoReservadoEvent(
            @JsonProperty("eventoId") UUID eventoId,
            @JsonProperty("evento") String evento,
            @JsonProperty("itens") List<ItemDoIngressoVO> itens) {

        this.eventoId = eventoId;
        this.evento = evento;
        this.itens = List.copyOf(itens);
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
}
