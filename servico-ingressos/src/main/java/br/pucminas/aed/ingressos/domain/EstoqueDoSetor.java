package br.pucminas.aed.ingressos.domain;

import java.util.List;
import java.util.Objects;

public final class EstoqueDoSetor {

    public static final long VERSAO_DE_STREAM_VAZIO = 0L;

    private final StreamDoEstoqueVO stream;

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
            case IngressoRetiradoEvent retirado -> this.retirados += retirado.getQuantidade();
            case IngressoDevolvidoEvent devolvido -> this.retirados -= devolvido.getQuantidade();
            case ReservaRecusadaEvent recusada -> this.recusas++;
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

    public IngressoDevolvidoEvent devolver(int quantidade, String origemEventoId, String motivo) {
        exigirQuantidadePositiva(quantidade);
        if (quantidade > retirados) {
            throw new IllegalStateException(
                    "devolucao de %d ingressos em %s, mas so %d foram retirados"
                            .formatted(quantidade, stream.id(), retirados));
        }
        return new IngressoDevolvidoEvent(quantidade, origemEventoId, motivo);
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
