package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.DeduplicacaoRepository;
import br.pucminas.aed.ingressos.domain.EstoqueDoSetor;
import br.pucminas.aed.ingressos.domain.EstoqueEvent;
import br.pucminas.aed.ingressos.domain.EventoDoEstoqueRepository;
import br.pucminas.aed.ingressos.domain.IngressoLiberadoEvent;
import br.pucminas.aed.ingressos.domain.IngressoReservadoEvent;
import br.pucminas.aed.ingressos.domain.ItemDoIngressoVO;
import br.pucminas.aed.ingressos.domain.ReservaRecusadaEvent;
import br.pucminas.aed.ingressos.domain.SituacaoDoEstoqueVO;
import br.pucminas.aed.ingressos.domain.StreamDoEstoqueVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class IngressoService {

    private static final Logger logger = LoggerFactory.getLogger(IngressoService.class);

    private final EventoDoEstoqueRepository eventoDoEstoqueRepository;
    private final DeduplicacaoRepository deduplicacaoRepository;

    public IngressoService(EventoDoEstoqueRepository eventoDoEstoqueRepository,
            DeduplicacaoRepository deduplicacaoRepository) {
        this.eventoDoEstoqueRepository = eventoDoEstoqueRepository;
        this.deduplicacaoRepository = deduplicacaoRepository;
    }

    @Transactional
    public void processarReserva(IngressoReservadoEvent mensagem) {
        if (!this.deduplicacaoRepository.registrar(mensagem.getEventoId())) {
            logger.info("mensagem repetida ignorada: eventoId={}", mensagem.getEventoId());
            return;
        }

        for (ItemDoIngressoVO item : mensagem.getItens()) {
            retirar(mensagem, item);
        }
    }

    @Transactional
    public void processarLiberacao(IngressoLiberadoEvent mensagem) {
        if (!this.deduplicacaoRepository.registrar(mensagem.getEventoId())) {
            logger.info("liberacao repetida ignorada: eventoId={}", mensagem.getEventoId());
            return;
        }

        var setores = mensagem.getItens().stream().map(ItemDoIngressoVO::getSetor).distinct().toList();
        for (String setor : setores) {
            liberar(mensagem.getEvento(), setor, mensagem.getReservaEventoId().toString(),
                    mensagem.getEventoId().toString(), mensagem.getMotivo());
        }
    }

    @Transactional
    public void liberar(String evento, String setor, String reservaEventoId, String liberacaoEventoId,
            String motivo) {
        StreamDoEstoqueVO stream = StreamDoEstoqueVO.de(evento, setor);
        EstoqueDoSetor estoque = carregar(stream);

        estoque.liberar(reservaEventoId, liberacaoEventoId, motivo).ifPresentOrElse(
                devolucao -> {
                    this.eventoDoEstoqueRepository.anexar(stream, estoque.getVersao(), List.of(devolucao));
                    logger.info("devolvidos {} ingresso(s) em {} da reserva {}",
                            devolucao.getQuantidade(), stream, reservaEventoId);
                },
                () -> logger.info("nada a devolver em {} para a reserva {}: ja liberada ou recusada",
                        stream, reservaEventoId));
    }

    public SituacaoDoEstoqueVO consultar(String evento, String setor) {
        StreamDoEstoqueVO stream = StreamDoEstoqueVO.de(evento, setor);
        var log = this.eventoDoEstoqueRepository.lerStream(stream);
        return SituacaoDoEstoqueVO.de(EstoqueDoSetor.reconstruir(stream, log), log);
    }

    private void retirar(IngressoReservadoEvent mensagem, ItemDoIngressoVO item) {
        StreamDoEstoqueVO stream = StreamDoEstoqueVO.de(mensagem.getEvento(), item.getSetor());
        EstoqueDoSetor estoque = carregar(stream);

        EstoqueEvent fato = estoque.retirar(item.getQuantidade(), mensagem.getEventoId().toString());
        this.eventoDoEstoqueRepository.anexar(stream, estoque.getVersao(), List.of(fato));

        if (fato instanceof ReservaRecusadaEvent recusa) {
            logger.warn("reserva recusada em {}: pedidos {}, disponivel {}",
                    stream, recusa.getQuantidadePedida(), recusa.getDisponivelNoMomento());
        }
    }

    private EstoqueDoSetor carregar(StreamDoEstoqueVO stream) {
        return EstoqueDoSetor.reconstruir(stream, this.eventoDoEstoqueRepository.lerStream(stream));
    }
}