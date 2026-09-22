package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.AgregacaoDeSetorVO;
import br.pucminas.aed.ingressos.domain.AgregacaoRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;

@Repository
public class AgregacaoJdbcRepository implements AgregacaoRepository {

    private final JdbcTemplate clienteJdbc;

    public AgregacaoJdbcRepository(JdbcTemplate clienteJdbc) {
        this.clienteJdbc = clienteJdbc;
    }

    @Override
    public void somarNaJanela(String evento, String setor, Instant janelaInicio, int quantidade) {
        String sql = "MERGE INTO agregacao_reserva_por_setor_janela "
                + "(evento, setor, janela_inicio, total_ingressos, atualizado_em) "
                + "KEY (evento, setor, janela_inicio) VALUES (?, ?, ?, "
                + "COALESCE((SELECT total_ingressos FROM agregacao_reserva_por_setor_janela "
                + "WHERE evento = ? AND setor = ? AND janela_inicio = ?), 0) + ?, ?)";
        this.clienteJdbc.update(sql,
                evento, setor, Timestamp.from(janelaInicio),
                evento, setor, Timestamp.from(janelaInicio),
                quantidade, OffsetDateTime.now());
    }

    @Override
    public List<AgregacaoDeSetorVO> listar(String evento) {
        if (evento == null) {
            String sql = "SELECT evento, setor, janela_inicio, total_ingressos "
                    + "FROM agregacao_reserva_por_setor_janela ORDER BY janela_inicio DESC";
            return this.clienteJdbc.query(sql, this::mapear);
        }
        String sql = "SELECT evento, setor, janela_inicio, total_ingressos "
                + "FROM agregacao_reserva_por_setor_janela WHERE evento = ? ORDER BY janela_inicio DESC";
        return this.clienteJdbc.query(sql, this::mapear, evento);
    }

    private AgregacaoDeSetorVO mapear(java.sql.ResultSet resultado, int linha) throws java.sql.SQLException {
        return new AgregacaoDeSetorVO(
                resultado.getString("evento"),
                resultado.getString("setor"),
                resultado.getTimestamp("janela_inicio").toInstant(),
                resultado.getInt("total_ingressos"));
    }
}
