package br.pucminas.aed.ingressos.domain;

public class ReservaAindaNaoProcessadaException extends RuntimeException {

    private final String streamId;
    private final String reservaEventoId;

    public ReservaAindaNaoProcessadaException(String streamId, String reservaEventoId) {
        super("liberacao da reserva %s chegou antes da reserva em %s".formatted(reservaEventoId, streamId));
        this.streamId = streamId;
        this.reservaEventoId = reservaEventoId;
    }

    public String getStreamId() {
        return streamId;
    }

    public String getReservaEventoId() {
        return reservaEventoId;
    }
}