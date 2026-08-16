package br.pucminas.aed.vendas.domain;

import java.math.BigDecimal;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Um item da reserva: o assento/setor pedido e a quantidade.
 *
 * Imutavel de proposito: campos private final, sem setter. Uma vez montado
 * o evento, nada no caminho ate o Kafka pode alterar o que foi decidido.
 */
public final class ItemDoIngressoVO {

    private final String setor;
    private final int quantidade;
    private final BigDecimal precoUnitario;

    @JsonCreator
    public ItemDoIngressoVO(
            @JsonProperty("setor") String setor,
            @JsonProperty("quantidade") int quantidade,
            @JsonProperty("precoUnitario") BigDecimal precoUnitario) {
        this.setor = Objects.requireNonNull(setor, "setor");
        if (quantidade <= 0) {
            throw new IllegalArgumentException("quantidade deve ser maior que zero");
        }
        this.quantidade = quantidade;
        this.precoUnitario = Objects.requireNonNull(precoUnitario, "precoUnitario");
    }

    public String getSetor() {
        return setor;
    }

    public int getQuantidade() {
        return quantidade;
    }

    public BigDecimal getPrecoUnitario() {
        return precoUnitario;
    }
}
