package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.ConcorrenciaNoStreamException;
import br.pucminas.aed.ingressos.domain.EstoqueEvent;
import br.pucminas.aed.ingressos.domain.EventoDoEstoqueRepository;
import br.pucminas.aed.ingressos.domain.EventoGravadoVO;
import br.pucminas.aed.ingressos.domain.IngressoDevolvidoEvent;
import br.pucminas.aed.ingressos.domain.IngressoRetiradoEvent;
import br.pucminas.aed.ingressos.domain.ReservaRecusadaEvent;
import br.pucminas.aed.ingressos.domain.SetorAbertoEvent;
import br.pucminas.aed.ingressos.domain.StreamDoEstoqueVO;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

@Repository
public class EventoDoEstoqueJdbcRepository implements EventoDoEstoqueRepository {

    private static final String COLUNAS = "sequencia, stream_id, versao, tipo, dados, gravado_em";

    private final JdbcTemplate clienteJdbc;
    private final ObjectMapper conversorJson;

    public EventoDoEstoqueJdbcRepository(JdbcTemplate clienteJdbc, ObjectMapper conversorJson) {
        this.clienteJdbc = clienteJdbc;
        this.conversorJson = conversorJson;
    }

    @Override
    public List<EventoGravadoVO> anexar(StreamDoEstoqueVO stream, long versaoEsperada,
            List<EstoqueEvent> eventos) {
        String sql = "INSERT INTO evento_do_estoque (stream_id, versao, tipo, dados, gravado_em) "
                + "VALUES (?, ?, ?, ?, ?)";
        OffsetDateTime gravadoEm = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
        long versao = versaoEsperada;
        try {
            for (EstoqueEvent evento : eventos) {
                versao++;
                this.clienteJdbc.update(sql, stream.id(), versao, evento.tipo(),
                        this.conversorJson.writeValueAsString(evento), gravadoEm);
            }
        } catch (DuplicateKeyException colisao) {
            throw new ConcorrenciaNoStreamException(stream.id(), versaoEsperada, colisao);
        }
        return lerStreamDesde(stream, versaoEsperada);
    }

    @Override
    public List<EventoGravadoVO> lerStream(StreamDoEstoqueVO stream) {
        String sql = "SELECT " + COLUNAS + " FROM evento_do_estoque WHERE stream_id = ? ORDER BY versao";
        return this.clienteJdbc.query(sql, mapeador(), stream.id());
    }

    @Override
    public List<EventoGravadoVO> lerDesde(long sequenciaExclusiva, int limite) {
        String sql = "SELECT " + COLUNAS + " FROM evento_do_estoque WHERE sequencia > ? "
                + "ORDER BY sequencia LIMIT ?";
        return this.clienteJdbc.query(sql, mapeador(), sequenciaExclusiva, limite);
    }

    @Override
    public long contar() {
        Long total = this.clienteJdbc.queryForObject("SELECT COUNT(1) FROM evento_do_estoque", Long.class);
        return total == null ? 0L : total;
    }

    private List<EventoGravadoVO> lerStreamDesde(StreamDoEstoqueVO stream, long versaoExclusiva) {
        String sql = "SELECT " + COLUNAS + " FROM evento_do_estoque "
                + "WHERE stream_id = ? AND versao > ? ORDER BY versao";
        return this.clienteJdbc.query(sql, mapeador(), stream.id(), versaoExclusiva);
    }

    private RowMapper<EventoGravadoVO> mapeador() {
        return (linha, numero) -> new EventoGravadoVO(
                linha.getLong("sequencia"),
                StreamDoEstoqueVO.doId(linha.getString("stream_id")),
                linha.getLong("versao"),
                desserializar(linha.getString("tipo"), linha.getString("dados")),
                linha.getObject("gravado_em", OffsetDateTime.class).toInstant());
    }

    private EstoqueEvent desserializar(String tipo, String dados) {
        return switch (tipo) {
            case SetorAbertoEvent.TIPO -> this.conversorJson.readValue(dados, SetorAbertoEvent.class);
            case IngressoRetiradoEvent.TIPO -> this.conversorJson.readValue(dados, IngressoRetiradoEvent.class);
            case IngressoDevolvidoEvent.TIPO -> this.conversorJson.readValue(dados, IngressoDevolvidoEvent.class);
            case ReservaRecusadaEvent.TIPO -> this.conversorJson.readValue(dados, ReservaRecusadaEvent.class);
            default -> throw new IllegalStateException("tipo de evento desconhecido no log: " + tipo);
        };
    }
}
