package br.pucminas.aed.ingressos.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * O item como ESTE lado precisa dele: setor e quantidade.
 *
 * O precoUnitario que vem no evento e ignorado aqui de proposito — quem
 * cuida de estoque nao precisa saber de preco, e campo desconhecido nao
 * pode quebrar o consumidor.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ItemDoIngressoVO {

    private final String setor;
    private final int quantidade;

    @JsonCreator
    public ItemDoIngressoVO(
            @JsonProperty("setor") String setor,
            @JsonProperty("quantidade") int quantidade) {
        this.setor = setor;
        this.quantidade = quantidade;
    }

    public String getSetor() {
        return setor;
    }

    public int getQuantidade() {
        return quantidade;
    }
}
