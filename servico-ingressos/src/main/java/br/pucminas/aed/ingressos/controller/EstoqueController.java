package br.pucminas.aed.ingressos.controller;

import br.pucminas.aed.ingressos.domain.SituacaoDoEstoqueVO;
import br.pucminas.aed.ingressos.service.IngressoService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/estoque")
public class EstoqueController {

    private final IngressoService ingressoService;

    public EstoqueController(IngressoService ingressoService) {
        this.ingressoService = ingressoService;
    }

    @GetMapping("/{evento}/{setor}")
    public SituacaoDoEstoqueVO situacao(@PathVariable String evento, @PathVariable String setor) {
        return ingressoService.consultar(evento, setor);
    }
}
