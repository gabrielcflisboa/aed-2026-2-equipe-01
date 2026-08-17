package br.pucminas.aed.ingressos.domain;

import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class IngressoReservaCompensadaEvent {

    private final UUID eventoId;
    private final List<ItemDoIngressoVO> itens;

    @JsonCreator
    public IngressoReservaCompensadaEvent(
            @JsonProperty("eventoId") UUID eventoId,
            @JsonProperty("itens") List<ItemDoIngressoVO> itens) {
        this.eventoId = eventoId;
        this.itens = List.copyOf(itens);
    }

    public UUID getEventoId() {
        return eventoId;
    }

    public List<ItemDoIngressoVO> getItens() {
        return itens;
    }
}