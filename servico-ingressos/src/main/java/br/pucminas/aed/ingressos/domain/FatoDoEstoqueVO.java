package br.pucminas.aed.ingressos.domain;

import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonFormat;

public final class FatoDoEstoqueVO {

    private final long versao;
    private final String tipo;
    private final EstoqueEvent dados;
    private final Instant gravadoEm;

    public FatoDoEstoqueVO(long versao, String tipo, EstoqueEvent dados, Instant gravadoEm) {
        this.versao = versao;
        this.tipo = Objects.requireNonNull(tipo, "tipo");
        this.dados = Objects.requireNonNull(dados, "dados");
        this.gravadoEm = Objects.requireNonNull(gravadoEm, "gravadoEm");
    }

    public static FatoDoEstoqueVO de(EventoGravadoVO gravado) {
        return new FatoDoEstoqueVO(gravado.getVersao(), gravado.getEvento().tipo(), gravado.getEvento(),
                gravado.getGravadoEm());
    }

    public long getVersao() {
        return versao;
    }

    public String getTipo() {
        return tipo;
    }

    public EstoqueEvent getDados() {
        return dados;
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    public Instant getGravadoEm() {
        return gravadoEm;
    }
}
