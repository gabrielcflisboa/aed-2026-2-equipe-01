package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.DisponibilidadeRepository;
import br.pucminas.aed.ingressos.domain.EventoGravadoVO;
import br.pucminas.aed.ingressos.domain.IngressoDevolvidoEvent;
import br.pucminas.aed.ingressos.domain.IngressoRetiradoEvent;
import br.pucminas.aed.ingressos.domain.ReservaRecusadaEvent;
import br.pucminas.aed.ingressos.domain.SetorAbertoEvent;
import org.springframework.stereotype.Service;

@Service
public class DisponibilidadeProjecaoService implements ProjecaoService {

    public static final String NOME = "disponibilidade_por_setor";

    private final DisponibilidadeRepository disponibilidadeRepository;

    public DisponibilidadeProjecaoService(DisponibilidadeRepository disponibilidadeRepository) {
        this.disponibilidadeRepository = disponibilidadeRepository;
    }

    @Override
    public String nome() {
        return NOME;
    }

    @Override
    public void aplicar(EventoGravadoVO gravado) {
        String evento = gravado.getStream().getEvento();
        String setor = gravado.getStream().getSetor();

        switch (gravado.getEvento()) {
            case SetorAbertoEvent aberto ->
                    this.disponibilidadeRepository.abrirSetor(evento, setor, aberto.getCapacidade());
            case IngressoRetiradoEvent retirado ->
                    this.disponibilidadeRepository.somarRetirados(evento, setor, retirado.getQuantidade());
            case IngressoDevolvidoEvent devolvido ->
                    this.disponibilidadeRepository.somarDevolvidos(evento, setor, devolvido.getQuantidade());
            case ReservaRecusadaEvent recusada -> {
            }
        }
    }

    @Override
    public void limpar() {
        this.disponibilidadeRepository.limpar();
    }
}
