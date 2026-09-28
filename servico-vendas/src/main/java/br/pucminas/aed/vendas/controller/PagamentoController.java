package br.pucminas.aed.vendas.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.pucminas.aed.vendas.service.GatewayDePagamentoService;

@RestController
@RequestMapping("/pagamentos")
public class PagamentoController {

    private final GatewayDePagamentoService gatewayDePagamentoService;

    public PagamentoController(GatewayDePagamentoService gatewayDePagamentoService) {
        this.gatewayDePagamentoService = gatewayDePagamentoService;
    }

    @PostMapping("/{compraId}/recusas")
    public ResponseEntity<Map<String, String>> recusar(@PathVariable String compraId) {
        var recusa = gatewayDePagamentoService.recusar(compraId);

        return ResponseEntity.accepted()
                .body(Map.of("eventoId", recusa.getEventoId(), "compraId", compraId));
    }
}