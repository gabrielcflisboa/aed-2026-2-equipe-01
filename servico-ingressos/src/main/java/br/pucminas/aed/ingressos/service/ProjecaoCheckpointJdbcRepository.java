package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.ProjecaoCheckpointRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ProjecaoCheckpointJdbcRepository implements ProjecaoCheckpointRepository {

    private final JdbcTemplate clienteJdbc;

    public ProjecaoCheckpointJdbcRepository(JdbcTemplate clienteJdbc) {
        this.clienteJdbc = clienteJdbc;
    }

    @Override
    public long ultimaSequencia(String projecao) {
        String sql = "SELECT ultima_sequencia FROM projecao_checkpoint WHERE projecao = ?";
        Long sequencia = this.clienteJdbc.query(sql,
                linhas -> linhas.next() ? linhas.getLong(1) : 0L, projecao);
        return sequencia == null ? 0L : sequencia;
    }

    @Override
    public void gravar(String projecao, long sequencia) {
        String sql = "MERGE INTO projecao_checkpoint (projecao, ultima_sequencia) KEY (projecao) VALUES (?, ?)";
        this.clienteJdbc.update(sql, projecao, sequencia);
    }
}
