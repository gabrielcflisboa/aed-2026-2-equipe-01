package br.pucminas.aed.ingressos.domain;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class EstoqueDoSetor {

    public static final long VERSAO_DE_STREAM_VAZIO = 0L;

    private final StreamDoEstoqueVO stream;
    private final Map<String, Integer> retiradosPorReserva = new HashMap<>();
    private final Set<String> reservasRecusadas = new HashSet<>();
    private final Set<String> reservasLiberadas = new HashSet<>();

    private long versao = VERSAO_DE_STREAM_VAZIO;
    private boolean aberto;
    private int capacidade;
    private int retirados;
    private int recusas;

    private EstoqueDoSetor(StreamDoEstoqueVO stream) {
        this.stream = Objects.requireNonNull(stream, "stream");
    }

    public static EstoqueDoSetor reconstruir(StreamDoEstoqueVO stream, List<EventoGravadoVO> log) {
        var estoque = new EstoqueDoSetor(stream);
        for (var gravado : Objects.requireNonNull(log, "log")) {
            estoque.aplicar(gravado.getVersao(), gravado.getEvento());
        }
        return estoque;
    }

    private void aplicar(long versaoDoEvento, EstoqueEvent evento) {
        switch (evento) {
            case SetorAbertoEvent aberto -> {
                this.aberto = true;
                this.capacidade = aberto.getCapacidade();
            }
            case IngressoRetiradoEvent retirado -> {
                this.retirados += retirado.getQuantidade();
                this.retiradosPorReserva.merge(retirado.getOrigemEventoId(), retirado.getQuantidade(), Integer::sum);
            }
            case IngressoDevolvidoEvent devolvido -> {
                this.retirados -= devolvido.getQuantidade();
                if (devolvido.getReservaEventoId() != null) {
                    this.reservasLiberadas.add(devolvido.getReservaEventoId());
                }
            }
            case ReservaRecusadaEvent recusada -> {
                this.recusas++;
                this.reservasRecusadas.add(recusada.getOrigemEventoId());
            }
        }
        this.versao = versaoDoEvento;
    }

    public EstoqueEvent retirar(int quantidade, String origemEventoId) {
        exigirQuantidadePositiva(quantidade);
        if (!aberto || quantidade > getDisponivel()) {
            return new ReservaRecusadaEvent(quantidade, getDisponivel(), origemEventoId);
        }
        return new IngressoRetiradoEvent(quantidade, origemEventoId);
    }

    public Optional<IngressoDevolvidoEvent> liberar(String reservaEventoId, String liberacaoEventoId, String motivo) {
        if (reservasLiberadas.contains(reservaEventoId)) {
            return Optional.empty();
        }
        var retiradosPelaReserva = retiradosPorReserva.get(reservaEventoId);
        if (retiradosPelaReserva != null) {
            return Optional.of(new IngressoDevolvidoEvent(retiradosPelaReserva, liberacaoEventoId, reservaEventoId, motivo));
        }
        if (reservasRecusadas.contains(reservaEventoId)) {
            return Optional.empty();
        }
        throw new ReservaAindaNaoProcessadaException(stream.id(), reservaEventoId);
    }

    private static void exigirQuantidadePositiva(int quantidade) {
        if (quantidade <= 0) {
            throw new IllegalArgumentException("quantidade deve ser maior que zero");
        }
    }

    public StreamDoEstoqueVO getStream() {
        return stream;
    }

    public long getVersao() {
        return versao;
    }

    public boolean isAberto() {
        return aberto;
    }

    public int getCapacidade() {
        return capacidade;
    }

    public int getRetirados() {
        return retirados;
    }

    public int getRecusas() {
        return recusas;
    }

    public int getDisponivel() {
        return capacidade - retirados;
    }
}