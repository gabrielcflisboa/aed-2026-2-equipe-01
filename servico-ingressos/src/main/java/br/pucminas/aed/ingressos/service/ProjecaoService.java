package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.EventoGravadoVO;

public interface ProjecaoService {

    String nome();

    void aplicar(EventoGravadoVO gravado);

    void limpar();
}
