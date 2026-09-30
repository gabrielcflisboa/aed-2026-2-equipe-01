package br.pucminas.aed.ingressos.domain;

import java.util.List;

public final class ResultadoDoReprocessamentoVO {

    private final List<String> republicados;
    private final List<String> naoEncontrados;

    public ResultadoDoReprocessamentoVO(List<String> republicados, List<String> naoEncontrados) {
        this.republicados = List.copyOf(republicados);
        this.naoEncontrados = List.copyOf(naoEncontrados);
    }

    public List<String> getRepublicados() {
        return republicados;
    }

    public List<String> getNaoEncontrados() {
        return naoEncontrados;
    }
}
