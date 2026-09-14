package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.DeduplicacaoRepository;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Repository
public class DeduplicacaoJdbcRepository implements DeduplicacaoRepository {

    private final JdbcTemplate clienteJdbc;

    public DeduplicacaoJdbcRepository(JdbcTemplate clienteJdbc) {
        this.clienteJdbc = clienteJdbc;
    }

    @Override
    public boolean registrar(UUID eventoId) {
        String sql = "INSERT INTO evento_processado (evento_id, processado_em) VALUES (?, ?)";
        try {
            this.clienteJdbc.update(sql, eventoId.toString(),
                    OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            return true;
        } catch (DuplicateKeyException jaEstava) {
            return false;
        }
    }

    @Override
    public boolean jaProcessado(UUID eventoId) {
        String sql = "SELECT COUNT(1) FROM evento_processado WHERE evento_id = ?";
        Integer quantidade = this.clienteJdbc.queryForObject(sql, Integer.class, eventoId.toString());
        return quantidade != null && quantidade > 0;
    }
}
