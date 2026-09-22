package br.pucminas.aed.ingressos.domain;

import java.time.Instant;
import java.util.List;

public interface AgregacaoRepository {

    void somarNaJanela(String evento, String setor, Instant janelaInicio, int quantidade);

    List<AgregacaoDeSetorVO> listar(String evento);
}
