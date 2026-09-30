package br.pucminas.aed.ingressos.domain;

public class ConcorrenciaNoStreamException extends RuntimeException {

    private final String streamId;
    private final long versaoEsperada;

    public ConcorrenciaNoStreamException(String streamId, long versaoEsperada, Throwable causa) {
        super("stream %s ja avancou alem da versao %d".formatted(streamId, versaoEsperada), causa);
        this.streamId = streamId;
        this.versaoEsperada = versaoEsperada;
    }

    public String getStreamId() {
        return streamId;
    }

    public long getVersaoEsperada() {
        return versaoEsperada;
    }
}
