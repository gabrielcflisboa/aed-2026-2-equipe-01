package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.IngressoConfig;
import br.pucminas.aed.ingressos.domain.EstoqueDoSetor;
import br.pucminas.aed.ingressos.domain.EventoDoEstoqueRepository;
import br.pucminas.aed.ingressos.domain.SetorAbertoEvent;
import br.pucminas.aed.ingressos.domain.StreamDoEstoqueVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class AberturaDeSetoresService implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(AberturaDeSetoresService.class);

    private final EventoDoEstoqueRepository eventoDoEstoqueRepository;
    private final IngressoConfig ingressoConfig;

    public AberturaDeSetoresService(EventoDoEstoqueRepository eventoDoEstoqueRepository,
            IngressoConfig ingressoConfig) {
        this.eventoDoEstoqueRepository = eventoDoEstoqueRepository;
        this.ingressoConfig = ingressoConfig;
    }

    @Override
    @Transactional
    public void run(ApplicationArguments argumentos) {
        this.ingressoConfig.getAbertura().forEach((evento, setores) ->
                setores.forEach((setor, capacidade) -> abrir(evento, setor, capacidade)));
    }

    public void abrir(String evento, String setor, int capacidade) {
        StreamDoEstoqueVO stream = StreamDoEstoqueVO.de(evento, setor);
        if (!this.eventoDoEstoqueRepository.lerStream(stream).isEmpty()) {
            return;
        }
        this.eventoDoEstoqueRepository.anexar(stream, EstoqueDoSetor.VERSAO_DE_STREAM_VAZIO,
                List.of(new SetorAbertoEvent(capacidade)));
        logger.info("setor aberto: {} com capacidade {}", stream, capacidade);
    }
}
