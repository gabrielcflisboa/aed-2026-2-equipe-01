package br.pucminas.aed.ingressos.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EstoqueDoSetorTest {

    private static final StreamDoEstoqueVO PISTA =
            StreamDoEstoqueVO.de("show-pucminas-2026", "PISTA");

    @Test
    @DisplayName("stream vazio: versao zero, setor fechado, nada disponivel")
    void streamVazio() {
        var estoque = EstoqueDoSetor.reconstruir(PISTA, List.of());

        assertThat(estoque.getVersao()).isEqualTo(EstoqueDoSetor.VERSAO_DE_STREAM_VAZIO);
        assertThat(estoque.isAberto()).isFalse();
        assertThat(estoque.getDisponivel()).isZero();
    }

    @Test
    @DisplayName("o estado e o resultado de aplicar o log inteiro, na ordem")
    void estadoVemDoLog() {
        var estoque = EstoqueDoSetor.reconstruir(PISTA, log(
                new SetorAbertoEvent(10),
                new IngressoRetiradoEvent(3, "msg-1"),
                new IngressoRetiradoEvent(2, "msg-2"),
                new IngressoDevolvidoEvent(1, "msg-1", "pagamento recusado")));

        assertThat(estoque.getCapacidade()).isEqualTo(10);
        assertThat(estoque.getRetirados()).isEqualTo(4);
        assertThat(estoque.getDisponivel()).isEqualTo(6);
        assertThat(estoque.getVersao()).isEqualTo(4);
    }

    @Test
    @DisplayName("pedido acima do disponivel vira ReservaRecusada, nao excecao")
    void pedidoAcimaDoDisponivelERecusado() {
        var estoque = EstoqueDoSetor.reconstruir(PISTA, log(
                new SetorAbertoEvent(4),
                new IngressoRetiradoEvent(3, "msg-1")));

        var fato = estoque.retirar(2, "msg-2");

        assertThat(fato).isInstanceOf(ReservaRecusadaEvent.class);
        var recusa = (ReservaRecusadaEvent) fato;
        assertThat(recusa.getQuantidadePedida()).isEqualTo(2);
        assertThat(recusa.getDisponivelNoMomento()).isEqualTo(1);
    }

    @Test
    @DisplayName("setor nunca aberto recusa qualquer retirada")
    void setorFechadoRecusa() {
        var estoque = EstoqueDoSetor.reconstruir(PISTA, List.of());

        assertThat(estoque.retirar(1, "msg-1")).isInstanceOf(ReservaRecusadaEvent.class);
    }

    @Test
    @DisplayName("pedido que cabe vira IngressoRetirado")
    void pedidoQueCabeEAceito() {
        var estoque = EstoqueDoSetor.reconstruir(PISTA, log(new SetorAbertoEvent(4)));

        var fato = estoque.retirar(4, "msg-1");

        assertThat(fato).isInstanceOf(IngressoRetiradoEvent.class);
        assertThat(((IngressoRetiradoEvent) fato).getQuantidade()).isEqualTo(4);
    }

    @Test
    @DisplayName("a compensacao nao devolve mais do que saiu")
    void compensacaoNaoInventaIngresso() {
        var estoque = EstoqueDoSetor.reconstruir(PISTA, log(
                new SetorAbertoEvent(10),
                new IngressoRetiradoEvent(2, "msg-1")));

        assertThatThrownBy(() -> estoque.devolver(3, "msg-1", "reserva expirada"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("so 2 foram retirados");
    }

    @Test
    @DisplayName("a decisao nao altera o agregado: quem muda o estado e o log")
    void decidirNaoMutaOAgregado() {
        var estoque = EstoqueDoSetor.reconstruir(PISTA, log(new SetorAbertoEvent(10)));

        estoque.retirar(3, "msg-1");
        estoque.retirar(3, "msg-2");

        assertThat(estoque.getRetirados()).isZero();
        assertThat(estoque.getDisponivel()).isEqualTo(10);
        assertThat(estoque.getVersao()).isEqualTo(1);
    }

    private static List<EventoGravadoVO> log(EstoqueEvent... eventos) {
        var gravados = new ArrayList<EventoGravadoVO>();
        for (var i = 0; i < eventos.length; i++) {
            gravados.add(new EventoGravadoVO(i + 1L, PISTA, i + 1L, eventos[i], Instant.EPOCH));
        }
        return gravados;
    }
}
