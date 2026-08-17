package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.IngressoReservadoEvent;
import br.pucminas.aed.ingressos.domain.IngressoReservaCompensadaEvent;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class IngressoService {

    private final IngressoJdbcRepository ingressoRepository;

    public IngressoService(IngressoJdbcRepository ingressoRepository) {
        this.ingressoRepository = ingressoRepository;
    }

    @Transactional
    public void processarReserva(IngressoReservadoEvent evento) {

        if (ingressoRepository.existeEvento(evento.getEventoId())) {
            return;
        }

        for (var item : evento.getItens()) {
            if (!ingressoRepository.debitarEstoque(item.getSetor(), item.getQuantidade())) {
                throw new IllegalStateException("estoque indisponivel para o setor " + item.getSetor());
            }
        }

        ingressoRepository.registrarEvento(evento.getEventoId());
    }

    @Transactional
    public void processarCompensacao(IngressoReservaCompensadaEvent evento) {
        if (ingressoRepository.existeEvento(evento.getEventoId())) {
            return;
        }

        for (var item : evento.getItens()) {
            ingressoRepository.devolverEstoque(item.getSetor(), item.getQuantidade());
        }

        ingressoRepository.registrarEvento(evento.getEventoId());
    }
}