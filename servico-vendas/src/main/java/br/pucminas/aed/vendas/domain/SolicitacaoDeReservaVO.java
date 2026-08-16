package br.pucminas.aed.vendas.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class SolicitacaoDeReservaVO {

    private final String compraId;
    private final String cpfComprador;
    private final String evento;
    private final List<ItemDoIngressoVO> itens;

    @JsonCreator
    public SolicitacaoDeReservaVO(
            @JsonProperty("compraId") String compraId,
            @JsonProperty("cpfComprador") String cpfComprador,
            @JsonProperty("evento") String evento,
            @JsonProperty("itens") List<ItemDoIngressoVO> itens) {
        this.compraId = Objects.requireNonNull(compraId, "compraId");
        this.cpfComprador = Objects.requireNonNull(cpfComprador, "cpfComprador");
        this.evento = Objects.requireNonNull(evento, "evento");
        this.itens = List.copyOf(new ArrayList<>(Objects.requireNonNull(itens, "itens")));
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
}
