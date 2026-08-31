package br.pucminas.aed.ingressos.controller;

import br.pucminas.aed.ingressos.domain.AgregacaoDeSetorVO;
import br.pucminas.aed.ingressos.service.AgregacaoDeReservasService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/agregacao")
public class AgregacaoController {

    private final AgregacaoDeReservasService agregacaoDeReservasService;

    public AgregacaoController(AgregacaoDeReservasService agregacaoDeReservasService) {
        this.agregacaoDeReservasService = agregacaoDeReservasService;
    }

    @GetMapping("/reservas-por-setor")
    public List<AgregacaoDeSetorVO> reservasPorSetor(@RequestParam(required = false) String evento) {
        return agregacaoDeReservasService.listar(evento);
    }
}
