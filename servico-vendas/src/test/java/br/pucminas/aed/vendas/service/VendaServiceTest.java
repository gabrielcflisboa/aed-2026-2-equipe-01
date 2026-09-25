package br.pucminas.aed.vendas.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

import br.pucminas.aed.vendas.VendaConfig;
import br.pucminas.aed.vendas.domain.CompraNaoEncontradaException;
import br.pucminas.aed.vendas.domain.ItemDoIngressoVO;
import br.pucminas.aed.vendas.domain.SolicitacaoDeReservaVO;

/**
 * Cobre a reacao do VendaService a uma recusa de pagamento ja decidida em
 * outro lugar (a decisao em si e papel do GatewayDePagamentoService).
 */
@ExtendWith(MockitoExtension.class)
class VendaServiceTest {

    @Mock
    private VendaCallbackService vendaCallbackService;

    @Mock
    private VendaCompensacaoCallbackService vendaCompensacaoCallbackService;

    private VendaService vendaService;

    @BeforeEach
    void preparar() {
        var vendaConfig = new VendaConfig();
        vendaConfig.setLimitePorCpf(4);
        vendaConfig.setSetores(Map.of("PISTA", 100));

        vendaService = new VendaService(vendaCallbackService, vendaCompensacaoCallbackService, vendaConfig);
    }

    @Test
    @DisplayName("compensarPagamentoRecusado publica o evento com o motivo recebido e remove a reserva do mapa")
    void compensarPublicaEventoComMotivoERemoveReserva() {
        var reserva = vendaService.reservar(solicitacao("compra-0001"));

        var compensacao = vendaService.compensarPagamentoRecusado("compra-0001", "pagamento recusado");

        assertThat(compensacao.getCompraId()).isEqualTo("compra-0001");
        assertThat(compensacao.getEvento()).isEqualTo("show-pucminas-2026");
        assertThat(compensacao.getMotivo()).isEqualTo("pagamento recusado");
        assertThat(compensacao.getEventoId()).isNotEqualTo(reserva.getEventoId());
        verify(vendaCompensacaoCallbackService).publicar(compensacao, "show-pucminas-2026");
    }

    @Test
    @DisplayName("compensar uma compraId desconhecida lanca CompraNaoEncontradaException")
    void compensarCompraDesconhecidaLancaExcecao() {
        assertThatThrownBy(() -> vendaService.compensarPagamentoRecusado("compra-inexistente", "motivo"))
                .isInstanceOf(CompraNaoEncontradaException.class);
    }

    @Test
    @DisplayName("compensar a mesma compra duas vezes lanca a excecao na segunda vez")
    void compensarDuasVezesLancaExcecaoNaSegunda() {
        vendaService.reservar(solicitacao("compra-0002"));

        vendaService.compensarPagamentoRecusado("compra-0002", "pagamento recusado");

        assertThatThrownBy(() -> vendaService.compensarPagamentoRecusado("compra-0002", "pagamento recusado"))
                .isInstanceOf(CompraNaoEncontradaException.class);
    }

    private static SolicitacaoDeReservaVO solicitacao(String compraId) {
        return new SolicitacaoDeReservaVO(compraId, "000.000.000-00", "show-pucminas-2026",
                List.of(new ItemDoIngressoVO("PISTA", 2, new BigDecimal("180.00"))));
    }
}
