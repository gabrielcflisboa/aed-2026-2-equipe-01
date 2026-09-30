package br.pucminas.aed.vendas.domain;

public class CompraNaoEncontradaException extends RuntimeException {

    private final String compraId;

    public CompraNaoEncontradaException(String compraId) {
        super("compra desconhecida pelo servico-vendas: %s".formatted(compraId));
        this.compraId = compraId;
    }

    public String getCompraId() {
        return compraId;
    }
}
