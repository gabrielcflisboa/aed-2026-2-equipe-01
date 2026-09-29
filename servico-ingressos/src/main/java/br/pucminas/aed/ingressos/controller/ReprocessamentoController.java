package br.pucminas.aed.ingressos.controller;

import br.pucminas.aed.ingressos.domain.EventoRetidoVO;
import br.pucminas.aed.ingressos.domain.PedidoDeReprocessamentoVO;
import br.pucminas.aed.ingressos.domain.ResultadoDoReprocessamentoVO;
import br.pucminas.aed.ingressos.service.ReprocessamentoService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/reprocessamentos")
public class ReprocessamentoController {

    private final ReprocessamentoService reprocessamentoService;

    public ReprocessamentoController(ReprocessamentoService reprocessamentoService) {
        this.reprocessamentoService = reprocessamentoService;
    }

    @GetMapping("/retidos")
    public List<EventoRetidoVO> retidos(@RequestParam String topicoDlq) {
        return reprocessamentoService.listar(topicoDlq);
    }

    @PostMapping
    public ResponseEntity<ResultadoDoReprocessamentoVO> reprocessar(@RequestBody PedidoDeReprocessamentoVO pedido) {
        return ResponseEntity.accepted().body(reprocessamentoService.reprocessar(pedido));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> pedidoInvalido(IllegalArgumentException recusa) {
        return Map.of(
                "erro", "pedido-de-reprocessamento-invalido",
                "mensagem", recusa.getMessage());
    }
}
