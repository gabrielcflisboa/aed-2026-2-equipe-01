package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.EventoGravadoVO;
import br.pucminas.aed.ingressos.domain.IngressoDevolvidoEvent;
import br.pucminas.aed.ingressos.domain.IngressoRetiradoEvent;
import br.pucminas.aed.ingressos.domain.OcupacaoRepository;
import br.pucminas.aed.ingressos.domain.ReservaRecusadaEvent;
import br.pucminas.aed.ingressos.domain.SetorAbertoEvent;
import org.springframework.stereotype.Service;

@Service
public class OcupacaoProjecaoService implements ProjecaoService {

    public static final String NOME = "ocupacao_por_evento";

    private final OcupacaoRepository ocupacaoRepository;

    public OcupacaoProjecaoService(OcupacaoRepository ocupacaoRepository) {
        this.ocupacaoRepository = ocupacaoRepository;
    }

    @Override
    public String nome() {
        return NOME;
    }

    @Override
    public void aplicar(EventoGravadoVO gravado) {
        String evento = gravado.getStream().getEvento();

        switch (gravado.getEvento()) {
            case SetorAbertoEvent aberto ->
                    this.ocupacaoRepository.somarCapacidade(evento, aberto.getCapacidade());
            case IngressoRetiradoEvent retirado ->
                    this.ocupacaoRepository.somarRetirados(evento, retirado.getQuantidade());
            case IngressoDevolvidoEvent devolvido ->
                    this.ocupacaoRepository.somarDevolvidos(evento, devolvido.getQuantidade());
            case ReservaRecusadaEvent recusada ->
                    this.ocupacaoRepository.somarRecusas(evento, 1);
        }
    }

    @Override
    public void limpar() {
        this.ocupacaoRepository.limpar();
    }
}
