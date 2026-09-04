package br.pucminas.aed.ingressos.domain;

import java.time.Instant;
import java.util.Objects;

public final class EventoGravadoVO {

    private final long sequencia;
    private final StreamDoEstoqueVO stream;
    private final long versao;
    private final EstoqueEvent evento;
    private final Instant gravadoEm;

    public EventoGravadoVO(long sequencia, StreamDoEstoqueVO stream, long versao,
            EstoqueEvent evento, Instant gravadoEm) {
        this.sequencia = sequencia;
        this.stream = Objects.requireNonNull(stream, "stream");
        this.versao = versao;
        this.evento = Objects.requireNonNull(evento, "evento");
        this.gravadoEm = Objects.requireNonNull(gravadoEm, "gravadoEm");
    }

    public long getSequencia() {
        return sequencia;
    }

    public StreamDoEstoqueVO getStream() {
        return stream;
    }

    public long getVersao() {
        return versao;
    }

    public EstoqueEvent getEvento() {
        return evento;
    }

    public Instant getGravadoEm() {
        return gravadoEm;
    }
}
