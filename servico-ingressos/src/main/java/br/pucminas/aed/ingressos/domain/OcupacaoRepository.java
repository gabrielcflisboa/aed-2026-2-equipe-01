package br.pucminas.aed.ingressos.domain;

public interface OcupacaoRepository {

    void somarCapacidade(String evento, int quantidade);

    void somarRetirados(String evento, int quantidade);

    void somarDevolvidos(String evento, int quantidade);

    void somarRecusas(String evento, int quantidade);

    void limpar();
}
