package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.AgregacaoRepository;
import br.pucminas.aed.ingressos.domain.DeduplicacaoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;

@Service
public class RetencaoDaDeduplicacaoService {

    private static final Logger logger = LoggerFactory.getLogger(RetencaoDaDeduplicacaoService.class);

    private final DeduplicacaoRepository deduplicacaoRepository;
    private final AgregacaoRepository agregacaoRepository;
    private final Duration retencao;

    public RetencaoDaDeduplicacaoService(DeduplicacaoRepository deduplicacaoRepository,
            AgregacaoRepository agregacaoRepository,
            @Value("${app.resiliencia.retencao-deduplicacao-dias}") long retencaoEmDias) {
        this.deduplicacaoRepository = deduplicacaoRepository;
        this.agregacaoRepository = agregacaoRepository;
        this.retencao = Duration.ofDays(retencaoEmDias);
    }

    @Scheduled(cron = "0 0 3 * * *")
    @Transactional
    public void esquecerEntregasAntigas() {
        var limite = Instant.now().minus(retencao);
        var doEstoque = this.deduplicacaoRepository.esquecerAntesDe(limite);
        var doAgregador = this.agregacaoRepository.esquecerEventosAntesDe(limite);
        logger.info("deduplicacao expurgada antes de {}: {} evento(s) do estoque, {} do agregador",
                limite, doEstoque, doAgregador);
    }
}
