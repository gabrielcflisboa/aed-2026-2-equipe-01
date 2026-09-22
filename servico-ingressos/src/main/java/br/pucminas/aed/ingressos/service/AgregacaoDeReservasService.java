package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.AgregacaoDeSetorVO;
import br.pucminas.aed.ingressos.domain.AgregacaoRepository;
import br.pucminas.aed.ingressos.domain.IngressoReservadoEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class AgregacaoDeReservasService {

    private final AgregacaoRepository agregacaoRepository;
    private final long tamanhoDaJanelaEmSegundos;

    public AgregacaoDeReservasService(AgregacaoRepository agregacaoRepository,
            @Value("${app.agregador.tamanho-da-janela-em-segundos}") long tamanhoDaJanelaEmSegundos) {
        this.agregacaoRepository = agregacaoRepository;
        this.tamanhoDaJanelaEmSegundos = tamanhoDaJanelaEmSegundos;
    }

    @Transactional
    public void agregar(IngressoReservadoEvent evento) {
        var janelaInicio = janelaDe(evento.getReservadoEm());
        var quantidadePorSetor = new LinkedHashMap<String, Integer>();
        for (var item : evento.getItens()) {
            quantidadePorSetor.merge(item.getSetor(), item.getQuantidade(), Integer::sum);
        }
        for (Map.Entry<String, Integer> entrada : quantidadePorSetor.entrySet()) {
            agregacaoRepository.somarNaJanela(evento.getEvento(), entrada.getKey(), janelaInicio, entrada.getValue());
        }
    }

    public List<AgregacaoDeSetorVO> listar(String evento) {
        return agregacaoRepository.listar(evento);
    }

    // Alinhamento por relogio (event time): janela = floor(reservadoEm, tamanho), nunca pela hora de subida do processo.
    private Instant janelaDe(Instant reservadoEm) {
        long epochSegundos = reservadoEm.getEpochSecond();
        long inicioDaJanela = epochSegundos - (epochSegundos % tamanhoDaJanelaEmSegundos);
        return Instant.ofEpochSecond(inicioDaJanela);
    }
}
