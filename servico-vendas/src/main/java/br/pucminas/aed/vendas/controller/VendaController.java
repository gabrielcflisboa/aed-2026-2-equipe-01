package br.pucminas.aed.vendas.controller;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import br.pucminas.aed.vendas.domain.CompraNaoEncontradaException;
import br.pucminas.aed.vendas.domain.LimiteDeIngressosExcedidoException;
import br.pucminas.aed.vendas.domain.SetorIndisponivelException;
import br.pucminas.aed.vendas.domain.SolicitacaoDeReservaVO;
import br.pucminas.aed.vendas.service.GatewayDePagamentoService;
import br.pucminas.aed.vendas.service.VendaService;

@RestController
@RequestMapping("/vendas")
public class VendaController {

    private final VendaService vendaService;
    private final GatewayDePagamentoService gatewayDePagamentoService;

    public VendaController(VendaService vendaService, GatewayDePagamentoService gatewayDePagamentoService) {
        this.vendaService = vendaService;
        this.gatewayDePagamentoService = gatewayDePagamentoService;
    }

    @PostMapping("/reservas")
    public ResponseEntity<Map<String, String>> reservar(@RequestBody SolicitacaoDeReservaVO solicitacao) {

        var evento = vendaService.reservar(solicitacao);

        return ResponseEntity.accepted()
                .body(Map.of("eventoId", evento.getEventoId(), "compraId", evento.getCompraId()));
    }

    /**
     * Simula o webhook onde um gateway de pagamento externo avisaria da
     * recusa. Quem decide a recusa e o GatewayDePagamentoService; este metodo
     * so liga essa decisao a reacao do VendaService.
     */
    @PostMapping("/reservas/{compraId}/compensacoes")
    public ResponseEntity<Map<String, String>> compensar(@PathVariable String compraId) {

        var motivo = gatewayDePagamentoService.recusar(compraId);
        var compensacao = vendaService.compensarPagamentoRecusado(compraId, motivo);

        return ResponseEntity.accepted()
                .body(Map.of("eventoId", compensacao.getEventoId(), "compraId", compraId));
    }

    @ExceptionHandler(CompraNaoEncontradaException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, Object> compraNaoEncontrada(CompraNaoEncontradaException recusa) {
        return Map.of(
                "erro", "compra-nao-encontrada",
                "mensagem", recusa.getMessage(),
                "compraId", recusa.getCompraId());
    }

    @ExceptionHandler(LimiteDeIngressosExcedidoException.class)
    @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
    public Map<String, Object> limiteExcedido(LimiteDeIngressosExcedidoException recusa) {
        return Map.of(
                "erro", "limite-por-cpf-excedido",
                "mensagem", recusa.getMessage(),
                "limite", recusa.getLimite(),
                "jaReservados", recusa.getJaReservados(),
                "pedidos", recusa.getPedidos());
    }

    @ExceptionHandler(SetorIndisponivelException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, Object> setorIndisponivel(SetorIndisponivelException recusa) {
        return Map.of(
                "erro", "setor-indisponivel",
                "mensagem", recusa.getMessage(),
                "setor", recusa.getSetor());
    }
}
