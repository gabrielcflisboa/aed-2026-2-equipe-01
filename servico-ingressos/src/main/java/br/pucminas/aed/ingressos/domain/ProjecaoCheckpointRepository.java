package br.pucminas.aed.ingressos.domain;

public interface ProjecaoCheckpointRepository {

    long ultimaSequencia(String projecao);

    void gravar(String projecao, long sequencia);
}
