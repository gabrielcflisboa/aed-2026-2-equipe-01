package br.pucminas.aed.vendas.service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.stereotype.Service;

import br.pucminas.aed.vendas.VendaConfig;
import br.pucminas.aed.vendas.domain.IngressoReservadoEvent;
import br.pucminas.aed.vendas.domain.ItemDoIngressoVO;
import br.pucminas.aed.vendas.domain.LimiteDeIngressosExcedidoException;
import br.pucminas.aed.vendas.domain.SetorIndisponivelException;
import br.pucminas.aed.vendas.domain.SolicitacaoDeReservaVO;

@Service
public class VendaService {

    private final VendaCallbackService vendaCallbackService;
    private final VendaConfig vendaConfig;
    private final ConcurrentMap<String, Integer> ingressosPorCpf = new ConcurrentHashMap<>();

    public VendaService(VendaCallbackService vendaCallbackService, VendaConfig vendaConfig) {
        this.vendaCallbackService = vendaCallbackService;
        this.vendaConfig = vendaConfig;
    }

    public IngressoReservadoEvent reservar(SolicitacaoDeReservaVO solicitacao) {
        var pedidoPorSetor = agruparPorSetor(solicitacao.getItens());

        conferirDisponibilidade(pedidoPorSetor);
        consumirCotaDoCpf(VendaConfig.normalizarCpf(solicitacao.getCpfComprador()),
                totalPedido(pedidoPorSetor));

        var evento = IngressoReservadoEvent.novo(
                solicitacao.getCompraId(),
                solicitacao.getCpfComprador(),
                solicitacao.getEvento(),
                solicitacao.getItens());

        vendaCallbackService.publicar(evento, evento.getEvento());

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
