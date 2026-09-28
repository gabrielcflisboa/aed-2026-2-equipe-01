package br.pucminas.aed.ingressos.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import br.pucminas.aed.ingressos.domain.EstoqueDoSetor;
import br.pucminas.aed.ingressos.domain.EventoDoEstoqueRepository;
import br.pucminas.aed.ingressos.domain.IngressoDevolvidoEvent;
import br.pucminas.aed.ingressos.domain.IngressoLiberadoEvent;
import br.pucminas.aed.ingressos.domain.IngressoReservadoEvent;
import br.pucminas.aed.ingressos.domain.IngressoRetiradoEvent;
import br.pucminas.aed.ingressos.domain.ItemDoIngressoVO;
import br.pucminas.aed.ingressos.domain.ReservaAindaNaoProcessadaException;
import br.pucminas.aed.ingressos.domain.ReservaRecusadaEvent;
import br.pucminas.aed.ingressos.domain.SetorAbertoEvent;
import br.pucminas.aed.ingressos.domain.StreamDoEstoqueVO;

@SpringBootTest
@Transactional
class LiberacaoDeReservaTest {

    private static final String EVENTO = "SHOW-LIBERACAO";

    @Autowired
    private AberturaDeSetoresService aberturaDeSetoresService;

    @Autowired
    private IngressoService ingressoService;

    @Autowired
    private EventoDoEstoqueRepository eventoDoEstoqueRepository;

    @Test
    @DisplayName("liberacao de reserva recusada nao devolve ingresso")
    void liberacaoDeReservaRecusadaNaoDevolve() {
        aberturaDeSetoresService.abrir(EVENTO, "PISTA", 2);

        var reservaId = UUID.randomUUID();
        ingressoService.processarReserva(reserva(reservaId, "PISTA", 5));

        ingressoService.processarLiberacao(
                liberacao(UUID.randomUUID(), reservaId, "PISTA", 5));

        var stream = StreamDoEstoqueVO.de(EVENTO, "PISTA");
        var log = eventoDoEstoqueRepository.lerStream(stream);

        assertThat(log).extracting(g -> g.getEvento().tipo())
                .containsExactly(
                        SetorAbertoEvent.TIPO,
                        ReservaRecusadaEvent.TIPO);

        var estoque = EstoqueDoSetor.reconstruir(stream, log);
        assertThat(estoque.getRetirados()).isZero();
        assertThat(estoque.getDisponivel()).isEqualTo(2);
    }

    @Test
    @DisplayName("duas liberacoes diferentes da mesma reserva devolvem uma unica vez")
    void duasLiberacoesDaMesmaReservaDevolvemUmaVez() {
        aberturaDeSetoresService.abrir(EVENTO, "PISTA", 10);

        var reservaId = UUID.randomUUID();
        ingressoService.processarReserva(reserva(reservaId, "PISTA", 3));

        ingressoService.processarLiberacao(
                liberacao(UUID.randomUUID(), reservaId, "PISTA", 3));

        ingressoService.processarLiberacao(
                liberacao(UUID.randomUUID(), reservaId, "PISTA", 3));

        var stream = StreamDoEstoqueVO.de(EVENTO, "PISTA");
        var log = eventoDoEstoqueRepository.lerStream(stream);

        assertThat(log).extracting(g -> g.getEvento().tipo())
                .containsExactly(
                        SetorAbertoEvent.TIPO,
                        IngressoRetiradoEvent.TIPO,
                        IngressoDevolvidoEvent.TIPO);

        var estoque = EstoqueDoSetor.reconstruir(stream, log);
        assertThat(estoque.getRetirados()).isZero();
        assertThat(estoque.getDisponivel()).isEqualTo(10);
    }

    @Test
    @DisplayName("liberacao de reserva com dois setores devolve em cada stream")
    void liberacaoComDoisSetoresDevolveEmCadaStream() {
        aberturaDeSetoresService.abrir(EVENTO, "PISTA", 10);
        aberturaDeSetoresService.abrir(EVENTO, "CAMAROTE", 10);

        var reservaId = UUID.randomUUID();

        ingressoService.processarReserva(
                reserva(
                        reservaId,
                        List.of(
                                new ItemDoIngressoVO("PISTA", 2),
                                new ItemDoIngressoVO("CAMAROTE", 1))));

        ingressoService.processarLiberacao(
                liberacao(
                        UUID.randomUUID(),
                        reservaId,
                        List.of(
                                new ItemDoIngressoVO("PISTA", 2),
                                new ItemDoIngressoVO("CAMAROTE", 1))));

        var streamPista = StreamDoEstoqueVO.de(EVENTO, "PISTA");
        var streamCamarote = StreamDoEstoqueVO.de(EVENTO, "CAMAROTE");

        var logPista = eventoDoEstoqueRepository.lerStream(streamPista);
        var logCamarote = eventoDoEstoqueRepository.lerStream(streamCamarote);

        assertThat(logPista).extracting(g -> g.getEvento().tipo())
                .containsExactly(
                        SetorAbertoEvent.TIPO,
                        IngressoRetiradoEvent.TIPO,
                        IngressoDevolvidoEvent.TIPO);

        assertThat(logCamarote).extracting(g -> g.getEvento().tipo())
                .containsExactly(
                        SetorAbertoEvent.TIPO,
                        IngressoRetiradoEvent.TIPO,
                        IngressoDevolvidoEvent.TIPO);

        var estoquePista = EstoqueDoSetor.reconstruir(streamPista, logPista);
        var estoqueCamarote = EstoqueDoSetor.reconstruir(streamCamarote, logCamarote);

        assertThat(estoquePista.getDisponivel()).isEqualTo(10);
        assertThat(estoqueCamarote.getDisponivel()).isEqualTo(10);
    }

    @Test
    @DisplayName("liberacao de reserva ainda nao processada lanca excecao")
    void liberacaoDeReservaAindaNaoProcessadaLancaExcecao() {
        aberturaDeSetoresService.abrir(EVENTO, "PISTA", 10);

        var reservaId = UUID.randomUUID();

        assertThatThrownBy(() -> ingressoService.processarLiberacao(
                liberacao(UUID.randomUUID(), reservaId, "PISTA", 3)))
                .isInstanceOf(ReservaAindaNaoProcessadaException.class);
    }

    private static IngressoReservadoEvent reserva(
            UUID eventoId,
            String setor,
            int quantidade) {

        return reserva(
                eventoId,
                List.of(new ItemDoIngressoVO(setor, quantidade)));
    }

    private static IngressoReservadoEvent reserva(
            UUID eventoId,
            List<ItemDoIngressoVO> itens) {

        return new IngressoReservadoEvent(
                eventoId,
                EVENTO,
                itens,
                Instant.now());
    }

    private static IngressoLiberadoEvent liberacao(
            UUID eventoId,
            UUID reservaEventoId,
            String setor,
            int quantidade) {

        return liberacao(
                eventoId,
                reservaEventoId,
                List.of(new ItemDoIngressoVO(setor, quantidade)));
    }

    private static IngressoLiberadoEvent liberacao(
            UUID eventoId,
            UUID reservaEventoId,
            List<ItemDoIngressoVO> itens) {

        return new IngressoLiberadoEvent(
                eventoId,
                reservaEventoId,
                "compra-teste",
                EVENTO,
                itens,
                "pagamento recusado",
                Instant.now());
    }
}