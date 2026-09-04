package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.OcupacaoRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OcupacaoJdbcRepository implements OcupacaoRepository {

    private final JdbcTemplate clienteJdbc;

    public OcupacaoJdbcRepository(JdbcTemplate clienteJdbc) {
        this.clienteJdbc = clienteJdbc;
    }

    @Override
    public void somarCapacidade(String evento, int quantidade) {
        somarNaColuna("capacidade_total", evento, quantidade);
    }

    @Override
    public void somarRetirados(String evento, int quantidade) {
        somarNaColuna("ingressos_retirados", evento, quantidade);
    }

    @Override
    public void somarDevolvidos(String evento, int quantidade) {
        somarNaColuna("ingressos_devolvidos", evento, quantidade);
    }

    @Override
    public void somarRecusas(String evento, int quantidade) {
        somarNaColuna("reservas_recusadas", evento, quantidade);
    }

    @Override
    public void limpar() {
        this.clienteJdbc.update("DELETE FROM ocupacao_por_evento");
    }

    private void somarNaColuna(String coluna, String evento, int quantidade) {
        garantirEvento(evento);
        String sql = "UPDATE ocupacao_por_evento SET " + coluna + " = " + coluna + " + ? WHERE evento = ?";
        this.clienteJdbc.update(sql, quantidade, evento);
        recalcularOcupacao(evento);
    }

    private void garantirEvento(String evento) {
        this.clienteJdbc.update("MERGE INTO ocupacao_por_evento (evento) KEY (evento) VALUES (?)", evento);
    }

    private void recalcularOcupacao(String evento) {
        String sql = "UPDATE ocupacao_por_evento SET ocupacao_pct = "
                + "CASE WHEN capacidade_total = 0 THEN 0 "
                + "ELSE (ingressos_retirados - ingressos_devolvidos) * 100 / capacidade_total END "
                + "WHERE evento = ?";
        this.clienteJdbc.update(sql, evento);
    }
}
