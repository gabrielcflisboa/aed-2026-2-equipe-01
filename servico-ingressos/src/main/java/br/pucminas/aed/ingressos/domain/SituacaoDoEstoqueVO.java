package br.pucminas.aed.ingressos.domain;

import java.util.List;
import java.util.Objects;

public final class SituacaoDoEstoqueVO {

    private final String evento;
    private final String setor;
    private final boolean aberto;
    private final int capacidade;
    private final int retirados;
    private final int disponivel;
    private final long versao;
    private final List<FatoDoEstoqueVO> historico;

    public SituacaoDoEstoqueVO(String evento, String setor, boolean aberto, int capacidade, int retirados,
            int disponivel, long versao, List<FatoDoEstoqueVO> historico) {
        this.evento = Objects.requireNonNull(evento, "evento");
        this.setor = Objects.requireNonNull(setor, "setor");
        this.aberto = aberto;
        this.capacidade = capacidade;
        this.retirados = retirados;
        this.disponivel = disponivel;
        this.versao = versao;
        this.historico = List.copyOf(Objects.requireNonNull(historico, "historico"));
    }

    public static SituacaoDoEstoqueVO de(EstoqueDoSetor estoque, List<EventoGravadoVO> log) {
        return new SituacaoDoEstoqueVO(
                estoque.getStream().getEvento(),
                estoque.getStream().getSetor(),
                estoque.isAberto(),
                estoque.getCapacidade(),
                estoque.getRetirados(),
                estoque.getDisponivel(),
                estoque.getVersao(),
                log.stream().map(FatoDoEstoqueVO::de).toList());
    }

    public String getEvento() {
        return evento;
    }

    public String getSetor() {
        return setor;
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

    public int getDisponivel() {
        return disponivel;
    }

    public long getVersao() {
        return versao;
    }

    public List<FatoDoEstoqueVO> getHistorico() {
        return historico;
    }
}
