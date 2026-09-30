package br.pucminas.aed.vendas.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class PagamentoRecusadoEvent {

    private final String eventoId;
    private final String compraId;
    private final String motivo;
    private final Instant recusadoEm;

    @JsonCreator
    public PagamentoRecusadoEvent(
            @JsonProperty("eventoId") String eventoId,
            @JsonProperty("compraId") String compraId,
            @JsonProperty("motivo") String motivo,
            @JsonProperty("recusadoEm") Instant recusadoEm) {
        this.eventoId = Objects.requireNonNull(eventoId, "eventoId");
        this.compraId = Objects.requireNonNull(compraId, "compraId");
        this.motivo = Objects.requireNonNull(motivo, "motivo");
        this.recusadoEm = Objects.requireNonNull(recusadoEm, "recusadoEm");
    }

    public static PagamentoRecusadoEvent novo(String compraId, String motivo) {
        return new PagamentoRecusadoEvent(UUID.randomUUID().toString(), compraId, motivo, Instant.now());
    }

    public String getEventoId() {
        return eventoId;
    }

    public String getCompraId() {
        return compraId;
    }

    public String getMotivo() {
        return motivo;
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    public Instant getRecusadoEm() {
        return recusadoEm;
    }
}