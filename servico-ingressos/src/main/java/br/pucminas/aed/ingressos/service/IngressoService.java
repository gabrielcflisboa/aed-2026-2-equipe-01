package br.pucminas.aed.ingressos.service;

import br.pucminas.aed.ingressos.domain.DeduplicacaoRepository;
import br.pucminas.aed.ingressos.domain.EstoqueDoSetor;
import br.pucminas.aed.ingressos.domain.EstoqueEvent;
import br.pucminas.aed.ingressos.domain.EventoDoEstoqueRepository;
import br.pucminas.aed.ingressos.domain.IngressoDevolvidoEvent;
import br.pucminas.aed.ingressos.domain.IngressoReservadoEvent;
import br.pucminas.aed.ingressos.domain.ItemDoIngressoVO;
import br.pucminas.aed.ingressos.domain.ReservaRecusadaEvent;
import br.pucminas.aed.ingressos.domain.StreamDoEstoqueVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

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
    public void compensar(String evento, String setor, int quantidade, UUID origemEventoId, String motivo) {
        StreamDoEstoqueVO stream = StreamDoEstoqueVO.de(evento, setor);
        EstoqueDoSetor estoque = carregar(stream);

        IngressoDevolvidoEvent devolucao =
                estoque.devolver(quantidade, origemEventoId.toString(), motivo);
        this.eventoDoEstoqueRepository.anexar(stream, estoque.getVersao(), List.of(devolucao));

        logger.info("devolvidos {} ingresso(s) em {}: {}", quantidade, stream, motivo);
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
