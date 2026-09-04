package br.pucminas.aed.ingressos.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import br.pucminas.aed.ingressos.domain.EventoDoEstoqueRepository;
import br.pucminas.aed.ingressos.domain.IngressoReservadoEvent;
import br.pucminas.aed.ingressos.domain.ItemDoIngressoVO;

@SpringBootTest
@Transactional
class ReconstrucaoDeProjecaoTest {

    private static final String EVENTO = "SHOW-PUCMINAS-2026";

    @Autowired
    private AberturaDeSetoresService aberturaDeSetoresService;

    @Autowired
    private IngressoService ingressoService;

    @Autowired
    private ReconstrucaoService reconstrucaoService;

    @Autowired
    private EventoDoEstoqueRepository eventoDoEstoqueRepository;

    @Autowired
    private JdbcTemplate clienteJdbc;

    @BeforeEach
    void montarHistoricoComOsQuatroFatos() {
        aberturaDeSetoresService.abrir(EVENTO, "PISTA", 10);
        aberturaDeSetoresService.abrir(EVENTO, "CAMAROTE", 4);

        var primeiraCompra = UUID.randomUUID();
        ingressoService.processarReserva(reserva(primeiraCompra, "PISTA", 3));
        ingressoService.processarReserva(reserva(UUID.randomUUID(), "CAMAROTE", 4));
        ingressoService.processarReserva(reserva(UUID.randomUUID(), "CAMAROTE", 1));
        ingressoService.compensar(EVENTO, "PISTA", 1, primeiraCompra, "pagamento recusado");

        reconstrucaoService.avancar();
    }

    @Test
    @DisplayName("apagar a projecao inteira e reconstruir pelo log da o mesmo resultado")
    void projecaoApagadaEReconstruidaChegaAoMesmoEstado() {
        var disponibilidadeAntes = disponibilidade();
        var ocupacaoAntes = ocupacao();
        var eventosNoLog = eventoDoEstoqueRepository.contar();

        assertThat(disponibilidadeAntes).isNotEmpty();
        assertThat(ocupacaoAntes).isNotEmpty();

        clienteJdbc.update("DELETE FROM disponibilidade_por_setor");
        clienteJdbc.update("DELETE FROM ocupacao_por_evento");
        assertThat(disponibilidade()).isEmpty();
        assertThat(ocupacao()).isEmpty();

        reconstrucaoService.reconstruir();

        assertThat(disponibilidade()).isEqualTo(disponibilidadeAntes);
        assertThat(ocupacao()).isEqualTo(ocupacaoAntes);
        assertThat(eventoDoEstoqueRepository.contar()).isEqualTo(eventosNoLog);
    }

    @Test
    @DisplayName("apagar as tabelas e o checkpoint basta: o catch-up normal reconstroi tudo")
    void apagarTabelasECheckpointReconstroiPeloCatchUp() {
        var disponibilidadeAntes = disponibilidade();
        var ocupacaoAntes = ocupacao();

        clienteJdbc.update("DELETE FROM disponibilidade_por_setor");
        clienteJdbc.update("DELETE FROM ocupacao_por_evento");
        clienteJdbc.update("DELETE FROM projecao_checkpoint");

        reconstrucaoService.avancar();

        assertThat(disponibilidade()).isEqualTo(disponibilidadeAntes);
        assertThat(ocupacao()).isEqualTo(ocupacaoAntes);
    }

    @Test
    @DisplayName("a reconstrucao e reproduzivel: duas vezes seguidas dao o mesmo resultado")
    void reconstrucaoEReproduzivel() {
        reconstrucaoService.reconstruir();
        var primeira = disponibilidade();
        var primeiraOcupacao = ocupacao();

        reconstrucaoService.reconstruir();

        assertThat(disponibilidade()).isEqualTo(primeira);
        assertThat(ocupacao()).isEqualTo(primeiraOcupacao);
    }

    @Test
    @DisplayName("escrita feita por fora na projecao nao sobrevive a reconstrucao")
    void escritaPorForaNaoSobrevive() {
        var correto = disponibilidade();

        clienteJdbc.update(
                "UPDATE disponibilidade_por_setor SET disponivel = 999 WHERE evento = ? AND setor = 'PISTA'",
                EVENTO);
        assertThat(disponibilidade()).isNotEqualTo(correto);

        reconstrucaoService.reconstruir();

        assertThat(disponibilidade()).isEqualTo(correto);
    }

    @Test
    @DisplayName("o historico montado chega aos numeros esperados nas duas telas")
    void numerosDasTelas() {
        assertThat(linhaDoSetor("PISTA"))
                .containsEntry("CAPACIDADE", 10)
                .containsEntry("RETIRADOS", 2)
                .containsEntry("DISPONIVEL", 8);

        assertThat(linhaDoSetor("CAMAROTE"))
                .containsEntry("RETIRADOS", 4)
                .containsEntry("DISPONIVEL", 0);

        assertThat(ocupacao().get(0))
                .containsEntry("CAPACIDADE_TOTAL", 14)
                .containsEntry("INGRESSOS_RETIRADOS", 7)
                .containsEntry("INGRESSOS_DEVOLVIDOS", 1)
                .containsEntry("RESERVAS_RECUSADAS", 1)
                .containsEntry("OCUPACAO_PCT", 42);
    }

    private List<Map<String, Object>> disponibilidade() {
        return clienteJdbc.queryForList(
                "SELECT * FROM disponibilidade_por_setor ORDER BY evento, setor");
    }

    private List<Map<String, Object>> ocupacao() {
        return clienteJdbc.queryForList("SELECT * FROM ocupacao_por_evento ORDER BY evento");
    }

    private Map<String, Object> linhaDoSetor(String setor) {
        return clienteJdbc.queryForMap(
                "SELECT * FROM disponibilidade_por_setor WHERE evento = ? AND setor = ?",
                EVENTO, setor);
    }

    private static IngressoReservadoEvent reserva(UUID eventoId, String setor, int quantidade) {
        return new IngressoReservadoEvent(eventoId, EVENTO,
                List.of(new ItemDoIngressoVO(setor, quantidade)));
    }
}
