package br.pucminas.aed.ingressos.domain;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class PedidoDeReprocessamentoVO {

    private final String topicoDlq;
    private final List<String> ceIds;

    @JsonCreator
    public PedidoDeReprocessamentoVO(
            @JsonProperty("topicoDlq") String topicoDlq,
            @JsonProperty("ceIds") List<String> ceIds) {
        this.topicoDlq = Objects.requireNonNull(topicoDlq, "topicoDlq");
        this.ceIds = List.copyOf(Objects.requireNonNull(ceIds, "ceIds"));
    }

    public String getTopicoDlq() {
        return topicoDlq;
    }

    public List<String> getCeIds() {
        return ceIds;
    }
}
