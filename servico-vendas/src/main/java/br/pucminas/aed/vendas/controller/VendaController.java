package br.pucminas.aed.vendas.controller;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import br.pucminas.aed.vendas.domain.LimiteDeIngressosExcedidoException;
import br.pucminas.aed.vendas.domain.SetorIndisponivelException;
import br.pucminas.aed.vendas.domain.SolicitacaoDeReservaVO;
import br.pucminas.aed.vendas.service.VendaService;

@RestController
@RequestMapping("/vendas")
public class VendaController {

    private final VendaService vendaService;

    public VendaController(VendaService vendaService) {
        this.vendaService = vendaService;
    }

    @PostMapping("/reservas")
    public ResponseEntity<Map<String, String>> reservar(@RequestBody SolicitacaoDeReservaVO solicitacao) {

        var evento = vendaService.reservar(solicitacao);

        return ResponseEntity.accepted()
                .body(Map.of("eventoId", evento.getEventoId(), "compraId", evento.getCompraId()));
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
