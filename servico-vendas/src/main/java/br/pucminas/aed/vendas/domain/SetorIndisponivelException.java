package br.pucminas.aed.vendas.domain;

public class SetorIndisponivelException extends RuntimeException {

    private final String setor;

    public SetorIndisponivelException(String setor, String motivo) {
        super("setor %s indisponivel: %s".formatted(setor, motivo));
        this.setor = setor;
    }

    public String getSetor() {
        return setor;
    }
}
