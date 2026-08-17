package br.pucminas.aed.vendas.service;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Service;
import br.pucminas.aed.vendas.VendaConfig;
import br.pucminas.aed.vendas.domain.IngressoReservaCompensadaEvent;
import br.pucminas.aed.vendas.domain.IngressoReservadoEvent;
import br.pucminas.aed.vendas.domain.ItemDoIngressoVO;
import br.pucminas.aed.vendas.domain.LimiteDeIngressosExcedidoException;
import br.pucminas.aed.vendas.domain.SetorIndisponivelException;
import br.pucminas.aed.vendas.domain.SolicitacaoDeReservaVO;

@Service
public class VendaService {

    private static final String TIPO_RESERVA = "vendas.ingresso.reservado.v1";
    private static final String TIPO_COMPENSACAO = "vendas.ingresso.reserva-compensada.v1";

    private final VendaCallbackService vendaCallbackService;
    private final VendaConfig vendaConfig;
    private final ConcurrentMap<String, Integer> ingressosPorCpf = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, IngressoReservadoEvent> reservasPorCompra = new ConcurrentHashMap<>();

    public VendaService(VendaCallbackService vendaCallbackService,
            VendaConfig vendaConfig) {
        this.vendaCallbackService = vendaCallbackService;
        this.vendaConfig = vendaConfig;
    }

    public IngressoReservadoEvent reservar(SolicitacaoDeReservaVO solicitacao) {
        var pedidoPorSetor = agruparPorSetor(solicitacao.getItens());

        conferirDisponibilidade(pedidoPorSetor);
        consumirCotaDoCpf(solicitacao.getCpfComprador(), totalPedido(pedidoPorSetor));

        var evento = IngressoReservadoEvent.novo(
                solicitacao.getCompraId(),
                solicitacao.getCpfComprador(),
                solicitacao.getEvento(),
                solicitacao.getItens());

        reservasPorCompra.put(evento.getCompraId(), evento);
        vendaCallbackService.publicar(vendaConfig.getTopicoReservas(), evento.getEvento(),
                evento.getEventoId(), evento.getReservadoEm(), TIPO_RESERVA, evento);

        return evento;
    }

    public IngressoReservaCompensadaEvent compensar(String compraId) {
        var reserva = reservasPorCompra.remove(compraId);
        if (reserva == null) {
            throw new IllegalArgumentException("reserva nao encontrada para a compra " + compraId);
        }

        ingressosPorCpf.computeIfPresent(reserva.getCpfComprador(),
                (cpf, reservados) -> reservados - totalPedido(agruparPorSetor(reserva.getItens())));

        var evento = IngressoReservaCompensadaEvent.novo(reserva);
        vendaCallbackService.publicar(vendaConfig.getTopicoCompensacoes(), evento.getEvento(),
                evento.getEventoId(), evento.getCompensadoEm(), TIPO_COMPENSACAO, evento);
        return evento;
    }

    private Map<String, Integer> agruparPorSetor(Iterable<ItemDoIngressoVO> itens) {
        var porSetor = new LinkedHashMap<String, Integer>();
        for (var item : itens) {
            porSetor.merge(VendaConfig.normalizar(item.getSetor()), item.getQuantidade(), Integer::sum);
        }
        return porSetor;
    }

    private void conferirDisponibilidade(Map<String, Integer> pedidoPorSetor) {
        pedidoPorSetor.forEach((setor, quantidade) -> {
            var capacidade = vendaConfig.getSetores().get(setor);
            if (capacidade == null) {
                throw new SetorIndisponivelException(setor, "setor nao existe neste evento");
            }
            if (quantidade > capacidade) {
                throw new SetorIndisponivelException(setor,
                        "capacidade e de %d ingressos, foram pedidos %d".formatted(capacidade, quantidade));
            }
        });
    }

    private void consumirCotaDoCpf(String cpf, int quantidadePedida) {
        var limite = vendaConfig.getLimitePorCpf();
        ingressosPorCpf.compute(cpf, (chave, jaReservados) -> {
            var atual = jaReservados == null ? 0 : jaReservados;
            if (atual + quantidadePedida > limite) {
                throw new LimiteDeIngressosExcedidoException(atual, quantidadePedida, limite);
            }
            return atual + quantidadePedida;
        });
    }

    private int totalPedido(Map<String, Integer> pedidoPorSetor) {
        return pedidoPorSetor.values().stream().mapToInt(Integer::intValue).sum();
    }

}
