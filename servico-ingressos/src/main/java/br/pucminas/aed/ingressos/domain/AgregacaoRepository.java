package br.pucminas.aed.ingressos.domain;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface AgregacaoRepository {

    boolean registrarEvento(UUID eventoId);

    void somarNaJanela(String evento, String setor, Instant janelaInicio, int quantidade);

    List<AgregacaoDeSetorVO> listar(String evento);
}
