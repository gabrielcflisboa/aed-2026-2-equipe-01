package br.pucminas.aed.vendas.service;

import org.springframework.stereotype.Service;

import br.pucminas.aed.vendas.domain.PagamentoRecusadoEvent;

@Service
public class GatewayDePagamentoService {

    private static final String MOTIVO_DA_RECUSA = "pagamento recusado pelo gateway (simulado)";

    private final PagamentoCallbackService pagamentoCallbackService;

    public GatewayDePagamentoService(PagamentoCallbackService pagamentoCallbackService) {
        this.pagamentoCallbackService = pagamentoCallbackService;
    }

    public PagamentoRecusadoEvent recusar(String compraId) {
        var recusa = PagamentoRecusadoEvent.novo(compraId, MOTIVO_DA_RECUSA);
        pagamentoCallbackService.publicar(recusa);
        return recusa;
    }
}