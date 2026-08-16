package br.pucminas.aed.vendas.controller;

import br.pucminas.aed.vendas.service.VendaService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/vendas")
public class VendaController {

    private final VendaService vendaService;

    public VendaController(VendaService vendaService) {
        this.vendaService = vendaService;
    }

    @PostMapping("/reservas")
    public ResponseEntity<Void> reservar(@RequestBody /* tipo da reserva */ request) {

        vendaService.reservar(request);

        return ResponseEntity.accepted().build();
    }
}