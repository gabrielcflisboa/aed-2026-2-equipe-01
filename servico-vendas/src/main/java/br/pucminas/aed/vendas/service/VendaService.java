package br.pucminas.aed.vendas.service;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import br.pucminas.aed.vendas.VendaConfig;
import br.pucminas.aed.vendas.domain.IngressoReservadoEvent;
import br.pucminas.aed.vendas.domain.ItemDoIngressoVO;
import br.pucminas.aed.vendas.domain.LimiteDeIngressosExcedidoException;
import br.pucminas.aed.vendas.domain.SetorIndisponivelException;
import br.pucminas.aed.vendas.domain.SolicitacaoDeReservaVO;

@Service
public class VendaService {

    /** Uma grafia so, versionada: dominio.entidade.fato.v1. */
    private static final String TIPO_DO_EVENTO = "vendas.ingresso.reservado.v1";
    private static final String ORIGEM_DO_EVENTO = "/servico-vendas";

    private final KafkaTemplate<String, IngressoReservadoEvent> clienteDoBroker;
    private final VendaCallbackService vendaCallbackService;
    private final VendaConfig vendaConfig;
    private final ConcurrentMap<String, Integer> ingressosPorCpf = new ConcurrentHashMap<>();

    public VendaService(KafkaTemplate<String, IngressoReservadoEvent> clienteDoBroker,
            VendaCallbackService vendaCallbackService,
            VendaConfig vendaConfig) {
        this.clienteDoBroker = clienteDoBroker;
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

        var registro = new ProducerRecord<>(vendaConfig.getTopico(), evento.getEvento(), evento);
        envelopar(registro.headers(), evento);

        vendaCallbackService.registrar(clienteDoBroker.send(registro), evento.getEventoId());

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

    private void envelopar(Headers cabecalhos, IngressoReservadoEvent evento) {
        cabecalhos.add("ce_specversion", texto("1.0"));
        cabecalhos.add("ce_id", texto(evento.getEventoId()));
        cabecalhos.add("ce_source", texto(ORIGEM_DO_EVENTO));
        cabecalhos.add("ce_type", texto(TIPO_DO_EVENTO));
        cabecalhos.add("ce_time", texto(Instant.now().toString()));
    }

    private byte[] texto(String valor) {
        return valor.getBytes(StandardCharsets.UTF_8);
    }
}
