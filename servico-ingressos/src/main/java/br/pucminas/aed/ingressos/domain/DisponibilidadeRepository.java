package br.pucminas.aed.ingressos.domain;

public interface DisponibilidadeRepository {

    void abrirSetor(String evento, String setor, int capacidade);

    void somarRetirados(String evento, String setor, int quantidade);

    void somarDevolvidos(String evento, String setor, int quantidade);

    void limpar();
}
