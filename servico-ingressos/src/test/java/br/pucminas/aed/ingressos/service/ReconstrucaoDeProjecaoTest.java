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
        var eventosNoLog = eventoDoEstoqueRepository.contar();

        assertThat(disponibilidadeAntes).isNotEmpty();

        clienteJdbc.update("DELETE FROM disponibilidade_por_setor");
        assertThat(disponibilidade()).isEmpty();

        reconstrucaoService.reconstruir();

        assertThat(disponibilidade()).isEqualTo(disponibilidadeAntes);
        assertThat(eventoDoEstoqueRepository.contar()).isEqualTo(eventosNoLog);
    }

    @Test
    @DisplayName("apagar a tabela e o checkpoint basta: o catch-up normal reconstroi tudo")
    void apagarTabelaECheckpointReconstroiPeloCatchUp() {
        var disponibilidadeAntes = disponibilidade();

        clienteJdbc.update("DELETE FROM disponibilidade_por_setor");
        clienteJdbc.update("DELETE FROM projecao_checkpoint");

        reconstrucaoService.avancar();

        assertThat(disponibilidade()).isEqualTo(disponibilidadeAntes);
    }

    @Test
    @DisplayName("a reconstrucao e reproduzivel: duas vezes seguidas dao o mesmo resultado")
    void reconstrucaoEReproduzivel() {
        reconstrucaoService.reconstruir();
        var primeira = disponibilidade();

        reconstrucaoService.reconstruir();

        assertThat(disponibilidade()).isEqualTo(primeira);
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
    @DisplayName("o historico montado chega aos numeros esperados na tabela de disponibilidade")
    void numerosDasTelas() {
        assertThat(linhaDoSetor("PISTA"))
                .containsEntry("CAPACIDADE", 10)
                .containsEntry("RETIRADOS", 2)
                .containsEntry("DISPONIVEL", 8);

        assertThat(linhaDoSetor("CAMAROTE"))
                .containsEntry("RETIRADOS", 4)
                .containsEntry("DISPONIVEL", 0);
    }

    @Test
    @DisplayName("processar um novo evento apos o avanco faz o proximo ciclo agendado atualizar a projecao")
    void novoEventoEProcessadoNoProximoCicloDoAgendador() {
        // Valida estado inicial do setor VIP (ainda não criado)
        assertThat(clienteJdbc.queryForList("SELECT * FROM disponibilidade_por_setor WHERE evento = ? AND setor = 'VIP'", EVENTO))
                .isEmpty();

        // 1. Gera um fato novo no log de eventos
        aberturaDeSetoresService.abrir(EVENTO, "VIP", 20);

        // 2. Executa a rotina agendada (equivalente ao disparo do @Scheduled)
        reconstrucaoService.avancar();

        // 3. Verifica se a projeção foi atualizada incrementalmente com o novo fato
        assertThat(linhaDoSetor("VIP"))
                .containsEntry("CAPACIDADE", 20)
                .containsEntry("RETIRADOS", 0)
                .containsEntry("DISPONIVEL", 20);
    }

    private List<Map<String, Object>> disponibilidade() {
        return clienteJdbc.queryForList(
                "SELECT * FROM disponibilidade_por_setor ORDER BY evento, setor");
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