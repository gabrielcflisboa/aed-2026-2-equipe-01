package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.EventoDoEstoqueRepository;
import br.pucminas.aed.ingressos.domain.EventoGravadoVO;
import br.pucminas.aed.ingressos.domain.ProjecaoCheckpointRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class ReconstrucaoService {

    private static final Logger logger = LoggerFactory.getLogger(ReconstrucaoService.class);

    private static final int TAMANHO_DO_LOTE = 500;

    private final EventoDoEstoqueRepository eventoDoEstoqueRepository;
    private final ProjecaoCheckpointRepository projecaoCheckpointRepository;
    private final List<ProjecaoService> projecoes;

    public ReconstrucaoService(EventoDoEstoqueRepository eventoDoEstoqueRepository,
            ProjecaoCheckpointRepository projecaoCheckpointRepository,
            List<ProjecaoService> projecoes) {
        this.eventoDoEstoqueRepository = eventoDoEstoqueRepository;
        this.projecaoCheckpointRepository = projecaoCheckpointRepository;
        this.projecoes = projecoes;
    }

    @Scheduled(fixedDelayString = "${app.projecoes.intervalo}",
            initialDelayString = "${app.projecoes.intervalo}")
    @Transactional
    public void avancar() {
        for (ProjecaoService projecao : this.projecoes) {
            int aplicados = avancarUmLote(projecao);
            if (aplicados > 0) {
                logger.info("projecao {} avancou {} evento(s)", projecao.nome(), aplicados);
            }
        }
    }

    @Transactional
    public long reconstruir() {
        logger.info("reconstrucao iniciada: {} evento(s) no log, {} projecao(oes)",
                this.eventoDoEstoqueRepository.contar(), this.projecoes.size());

        for (ProjecaoService projecao : this.projecoes) {
            projecao.limpar();
            this.projecaoCheckpointRepository.gravar(projecao.nome(), 0L);
        }

        long reaplicados = 0L;
        for (ProjecaoService projecao : this.projecoes) {
            int lote;
            do {
                lote = avancarUmLote(projecao);
                reaplicados += lote;
            } while (lote > 0);
        }

        logger.info("reconstrucao concluida: {} aplicacao(oes) de evento", reaplicados);
        return reaplicados;
    }

    private int avancarUmLote(ProjecaoService projecao) {
        long checkpoint = this.projecaoCheckpointRepository.ultimaSequencia(projecao.nome());
        List<EventoGravadoVO> lote = this.eventoDoEstoqueRepository.lerDesde(checkpoint, TAMANHO_DO_LOTE);
        if (lote.isEmpty()) {
            return 0;
        }
        for (EventoGravadoVO gravado : lote) {
            projecao.aplicar(gravado);
        }
        this.projecaoCheckpointRepository.gravar(projecao.nome(), lote.get(lote.size() - 1).getSequencia());
        return lote.size();
    }
}
