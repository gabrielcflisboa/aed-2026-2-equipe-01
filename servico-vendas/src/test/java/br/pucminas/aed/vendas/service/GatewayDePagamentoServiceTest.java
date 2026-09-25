package br.pucminas.aed.vendas.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class GatewayDePagamentoServiceTest {

    private final GatewayDePagamentoService gatewayDePagamentoService = new GatewayDePagamentoService();

    @Test
    @DisplayName("recusar devolve um motivo nao vazio")
    void recusarDevolveMotivoNaoVazio() {
        var motivo = gatewayDePagamentoService.recusar("compra-0001");

        assertThat(motivo).isNotBlank();
    }
}
