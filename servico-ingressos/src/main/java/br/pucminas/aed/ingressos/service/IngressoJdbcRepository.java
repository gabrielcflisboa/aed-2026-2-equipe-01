package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.IngressoRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.UUID;

@Repository
public class IngressoJdbcRepository implements IngressoRepository {

    private final JdbcTemplate clienteJdbc;

    public IngressoJdbcRepository(JdbcTemplate clienteJdbc) {
        this.clienteJdbc = clienteJdbc;
    }

    @Override
    public boolean existeEvento(UUID eventoId) {
        String sql = "SELECT COUNT(1) FROM evento_processado WHERE evento_id = ?";
        Integer quantidade = this.clienteJdbc.queryForObject(sql, Integer.class, eventoId.toString());
        return quantidade != null && quantidade > 0;
    }

    @Override
    public boolean debitarEstoque(String setor, int quantidade) {
        String sql = "UPDATE estoque_setor SET quantidade_disponivel = quantidade_disponivel - ? " +
                "WHERE setor = ? AND quantidade_disponivel >= ?";
        int linhasAfetadas = this.clienteJdbc.update(sql, quantidade, setor, quantidade);
        return linhasAfetadas > 0;
    }

    @Override
    public void devolverEstoque(String setor, int quantidade) {
        String sql = "UPDATE estoque_setor SET quantidade_disponivel = quantidade_disponivel + ? WHERE setor = ?";
        this.clienteJdbc.update(sql, setor, quantidade);
    }

    @Override
    public void registrarEvento(UUID eventoId) {
        String sql = "INSERT INTO evento_processado (evento_id, processado_em) VALUES (?, ?)";
        this.clienteJdbc.update(sql, eventoId.toString(), OffsetDateTime.now());
    }
}