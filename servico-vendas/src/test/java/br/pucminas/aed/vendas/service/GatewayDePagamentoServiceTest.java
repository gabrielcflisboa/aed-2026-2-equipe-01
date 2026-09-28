package br.pucminas.aed.vendas.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GatewayDePagamentoServiceTest {

    @Mock
    private PagamentoCallbackService pagamentoCallbackService;

    @Test
    @DisplayName("recusar publica a recusa da compra pedida, com motivo")
    void recusarPublicaARecusa() {
        var gateway =
                new GatewayDePagamentoService(pagamentoCallbackService);

        var recusa = gateway.recusar("compra-0001");

        assertThat(recusa).isNotNull();
        assertThat(recusa.getEventoId()).isNotBlank();
        assertThat(recusa.getCompraId()).isEqualTo("compra-0001");
        assertThat(recusa.getMotivo()).isNotBlank();
        assertThat(recusa.getRecusadoEm()).isNotNull();

        verify(pagamentoCallbackService).publicar(recusa);
    }
}