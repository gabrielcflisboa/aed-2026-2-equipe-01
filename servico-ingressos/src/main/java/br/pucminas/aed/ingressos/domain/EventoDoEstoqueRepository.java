package br.pucminas.aed.ingressos.domain;

import java.util.List;

public interface EventoDoEstoqueRepository {

    List<EventoGravadoVO> anexar(StreamDoEstoqueVO stream, long versaoEsperada, List<EstoqueEvent> eventos);

    List<EventoGravadoVO> lerStream(StreamDoEstoqueVO stream);

    List<EventoGravadoVO> lerDesde(long sequenciaExclusiva, int limite);

    long contar();
}
