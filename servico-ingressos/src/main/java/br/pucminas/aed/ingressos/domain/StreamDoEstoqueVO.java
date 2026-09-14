package br.pucminas.aed.ingressos.domain;

import java.util.Locale;
import java.util.Objects;

public final class StreamDoEstoqueVO {

    private static final String SEPARADOR = "::";

    private final String evento;
    private final String setor;

    private StreamDoEstoqueVO(String evento, String setor) {
        this.evento = evento;
        this.setor = setor;
    }

    public static StreamDoEstoqueVO de(String evento, String setor) {
        return new StreamDoEstoqueVO(
                normalizar(Objects.requireNonNull(evento, "evento")),
                normalizar(Objects.requireNonNull(setor, "setor")));
    }

    public static StreamDoEstoqueVO doId(String id) {
        var partes = Objects.requireNonNull(id, "id").split(SEPARADOR, 2);
        if (partes.length != 2) {
            throw new IllegalArgumentException("id de stream invalido: " + id);
        }
        return new StreamDoEstoqueVO(partes[0], partes[1]);
    }

    private static String normalizar(String valor) {
        return valor.trim().toUpperCase(Locale.ROOT);
    }

    public String id() {
        return evento + SEPARADOR + setor;
    }

    public String getEvento() {
        return evento;
    }

    public String getSetor() {
        return setor;
    }

    @Override
    public boolean equals(Object outro) {
        return outro instanceof StreamDoEstoqueVO s
                && s.evento.equals(evento)
                && s.setor.equals(setor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(evento, setor);
    }

    @Override
    public String toString() {
        return id();
    }
}
