package br.pucminas.aed.ingressos.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import br.pucminas.aed.ingressos.domain.EstoqueDoSetor;
import br.pucminas.aed.ingressos.domain.EventoDoEstoqueRepository;
import br.pucminas.aed.ingressos.domain.IngressoReservadoEvent;
import br.pucminas.aed.ingressos.domain.IngressoRetiradoEvent;
import br.pucminas.aed.ingressos.domain.ItemDoIngressoVO;
import br.pucminas.aed.ingressos.domain.SetorAbertoEvent;
import br.pucminas.aed.ingressos.domain.StreamDoEstoqueVO;

@SpringBootTest
@Transactional
class IngressoServiceIdempotenciaTest {

    private static final String EVENTO = "SHOW-IDEMPOTENCIA";

    @Autowired
    private AberturaDeSetoresService aberturaDeSetoresService;

    @Autowired
    private IngressoService ingressoService;

    @Autowired
    private EventoDoEstoqueRepository eventoDoEstoqueRepository;

    @Test
    @DisplayName("mesmo evento entregue 3x: um unico IngressoRetirado no log")
    void mesmoEventoTresVezesEfeitoUnico() {
        aberturaDeSetoresService.abrir(EVENTO, "PISTA", 10);
        var mensagem = reserva(UUID.randomUUID(), "PISTA", 3);

        ingressoService.processarReserva(mensagem);
        ingressoService.processarReserva(mensagem);
        ingressoService.processarReserva(mensagem);

        var stream = StreamDoEstoqueVO.de(EVENTO, "PISTA");
        var log = eventoDoEstoqueRepository.lerStream(stream);

        assertThat(log).extracting(g -> g.getEvento().tipo())
                .containsExactly(SetorAbertoEvent.TIPO, IngressoRetiradoEvent.TIPO);

        var estoque = EstoqueDoSetor.reconstruir(stream, log);
        assertThat(estoque.getRetirados()).isEqualTo(3);
        assertThat(estoque.getDisponivel()).isEqualTo(7);
    }

    @Test
    @DisplayName("mensagens distintas sobre o mesmo setor somam, uma a uma")
    void mensagensDistintasSomam() {
        aberturaDeSetoresService.abrir(EVENTO, "CAMAROTE", 10);

        ingressoService.processarReserva(reserva(UUID.randomUUID(), "CAMAROTE", 2));
        ingressoService.processarReserva(reserva(UUID.randomUUID(), "CAMAROTE", 3));

        var stream = StreamDoEstoqueVO.de(EVENTO, "CAMAROTE");
        var estoque = EstoqueDoSetor.reconstruir(stream, eventoDoEstoqueRepository.lerStream(stream));

        assertThat(estoque.getVersao()).isEqualTo(3);
        assertThat(estoque.getDisponivel()).isEqualTo(5);
    }

    private static IngressoReservadoEvent reserva(UUID eventoId, String setor, int quantidade) {
        return new IngressoReservadoEvent(eventoId, EVENTO,
                List.of(new ItemDoIngressoVO(setor, quantidade)));
    }
}
