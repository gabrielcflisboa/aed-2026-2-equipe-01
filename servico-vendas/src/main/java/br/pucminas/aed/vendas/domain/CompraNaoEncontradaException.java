package br.pucminas.aed.vendas.domain;

public class CompraNaoEncontradaException extends RuntimeException {

    private final String compraId;

    public CompraNaoEncontradaException(String compraId) {
        super("compra nao encontrada ou ja compensada: %s".formatted(compraId));
        this.compraId = compraId;
    }

    public String getCompraId() {
        return compraId;
    }
}
