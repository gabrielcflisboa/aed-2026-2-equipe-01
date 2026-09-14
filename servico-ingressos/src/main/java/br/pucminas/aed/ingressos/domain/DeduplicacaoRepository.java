package br.pucminas.aed.ingressos.domain;

import java.util.UUID;

public interface DeduplicacaoRepository {

    boolean registrar(UUID eventoId);

    boolean jaProcessado(UUID eventoId);
}
