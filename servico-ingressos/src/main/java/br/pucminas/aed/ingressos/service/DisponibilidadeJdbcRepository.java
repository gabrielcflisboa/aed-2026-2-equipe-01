package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.DisponibilidadeRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class DisponibilidadeJdbcRepository implements DisponibilidadeRepository {

    private final JdbcTemplate clienteJdbc;

    public DisponibilidadeJdbcRepository(JdbcTemplate clienteJdbc) {
        this.clienteJdbc = clienteJdbc;
    }

    @Override
    public void abrirSetor(String evento, String setor, int capacidade) {
        String sql = "MERGE INTO disponibilidade_por_setor "
                + "(evento, setor, capacidade, retirados, disponivel) "
                + "KEY (evento, setor) VALUES (?, ?, ?, 0, ?)";
        this.clienteJdbc.update(sql, evento, setor, capacidade, capacidade);
    }

    @Override
    public void somarRetirados(String evento, String setor, int quantidade) {
        String sql = "UPDATE disponibilidade_por_setor "
                + "SET retirados = retirados + ?, disponivel = disponivel - ? "
                + "WHERE evento = ? AND setor = ?";
        this.clienteJdbc.update(sql, quantidade, quantidade, evento, setor);
    }

    @Override
    public void somarDevolvidos(String evento, String setor, int quantidade) {
        String sql = "UPDATE disponibilidade_por_setor "
                + "SET retirados = retirados - ?, disponivel = disponivel + ? "
                + "WHERE evento = ? AND setor = ?";
        this.clienteJdbc.update(sql, quantidade, quantidade, evento, setor);
    }

    @Override
    public void limpar() {
        this.clienteJdbc.update("DELETE FROM disponibilidade_por_setor");
    }
}
