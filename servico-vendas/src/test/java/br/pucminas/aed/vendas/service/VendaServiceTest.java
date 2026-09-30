package br.pucminas.aed.vendas.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.KafkaException;

import br.pucminas.aed.vendas.VendaConfig;
import br.pucminas.aed.vendas.domain.CompraNaoEncontradaException;
import br.pucminas.aed.vendas.domain.ItemDoIngressoVO;
import br.pucminas.aed.vendas.domain.PagamentoRecusadoEvent;
import br.pucminas.aed.vendas.domain.SolicitacaoDeReservaVO;

@ExtendWith(MockitoExtension.class)
class VendaServiceTest {

    @Mock
    private VendaCallbackService vendaCallbackService;

    @Mock
    private VendaLiberacaoCallbackService vendaLiberacaoCallbackService;

    private VendaService vendaService;

    @BeforeEach
    void preparar() {
        var vendaConfig = new VendaConfig();
        vendaConfig.setLimitePorCpf(4);
        vendaConfig.setSetores(Map.of("PISTA", 100));

        vendaService = new VendaService(vendaCallbackService, vendaLiberacaoCallbackService, vendaConfig);
    }

    @Test
    @DisplayName("a recusa libera a reserva certa e aponta para a recusa que a causou")
    void recusaLiberaAReservaCerta() {
        var reserva = vendaService.reservar(solicitacao("compra-0001"));
        var recusa = PagamentoRecusadoEvent.novo("compra-0001", "pagamento recusado");

        var liberacao = vendaService.liberarReserva(recusa).orElseThrow();

        assertThat(liberacao.getReservaEventoId()).isEqualTo(reserva.getEventoId());
        assertThat(liberacao.getPagamentoEventoId()).isEqualTo(recusa.getEventoId());
        assertThat(liberacao.getEventoId()).isNotEqualTo(reserva.getEventoId());
        assertThat(liberacao.getMotivo()).isEqualTo("pagamento recusado");
        verify(vendaLiberacaoCallbackService).publicar(liberacao, "show-pucminas-2026");
    }

    @Test
    @DisplayName("a segunda recusa da mesma compra nao publica outra liberacao")
    void segundaRecusaNaoPublicaDeNovo() {
        vendaService.reservar(solicitacao("compra-0002"));
        vendaService.liberarReserva(PagamentoRecusadoEvent.novo("compra-0002", "pagamento recusado"));

        var segunda = vendaService.liberarReserva(PagamentoRecusadoEvent.novo("compra-0002", "pagamento recusado"));

        assertThat(segunda).isEmpty();
        verify(vendaLiberacaoCallbackService, times(1)).publicar(any(), any());
    }

    @Test
    @DisplayName("recusa de compra desconhecida lanca CompraNaoEncontradaException")
    void recusaDeCompraDesconhecida() {
        var recusa = PagamentoRecusadoEvent.novo("compra-inexistente", "pagamento recusado");

        assertThatThrownBy(() -> vendaService.liberarReserva(recusa))
                .isInstanceOf(CompraNaoEncontradaException.class);
    }

    @Test
    @DisplayName("se a publicacao falha, a reserva continua e a proxima tentativa libera")
    void falhaAoPublicarMantemAReserva() {
        vendaService.reservar(solicitacao("compra-0003"));
        var recusa = PagamentoRecusadoEvent.novo("compra-0003", "pagamento recusado");
        doThrow(new KafkaException("broker fora"))
                .doNothing()
                .when(vendaLiberacaoCallbackService).publicar(any(), any());

        assertThatThrownBy(() -> vendaService.liberarReserva(recusa)).isInstanceOf(KafkaException.class);

        assertThat(vendaService.liberarReserva(recusa)).isPresent();
    }

    @Test
    @DisplayName("liberar devolve a cota do CPF")
    void liberarDevolveACotaDoCpf() {
        vendaService.reservar(solicitacao("compra-0004"));
        vendaService.reservar(solicitacao("compra-0005"));
        vendaService.liberarReserva(PagamentoRecusadoEvent.novo("compra-0004", "pagamento recusado"));

        assertThat(vendaService.reservar(solicitacao("compra-0006")).getCompraId()).isEqualTo("compra-0006");
    }

    @Test
    @DisplayName("um compraId reaproveitado em nova reserva pode ser liberado de novo")
    void compraIdReaproveitadoPodeSerLiberado() {
        vendaService.reservar(solicitacao("compra-0007"));
        vendaService.liberarReserva(PagamentoRecusadoEvent.novo("compra-0007", "pagamento recusado"));
        vendaService.reservar(solicitacao("compra-0007"));

        assertThat(vendaService.liberarReserva(PagamentoRecusadoEvent.novo("compra-0007", "pagamento recusado")))
                .isPresent();
    }

    private static SolicitacaoDeReservaVO solicitacao(String compraId) {
        return new SolicitacaoDeReservaVO(compraId, "000.000.000-00", "show-pucminas-2026",
                List.of(new ItemDoIngressoVO("PISTA", 2, new BigDecimal("180.00"))));
    }
}