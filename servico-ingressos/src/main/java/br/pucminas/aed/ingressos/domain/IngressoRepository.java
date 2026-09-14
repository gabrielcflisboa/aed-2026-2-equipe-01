package br.pucminas.aed.ingressos.domain;

import java.util.UUID;

public interface IngressoRepository {
    boolean existeEvento(UUID eventoId);

    boolean debitarEstoque(String setor, int quantidade);

    void devolverEstoque(String setor, int quantidade);

    void registrarEvento(UUID eventoId);
}