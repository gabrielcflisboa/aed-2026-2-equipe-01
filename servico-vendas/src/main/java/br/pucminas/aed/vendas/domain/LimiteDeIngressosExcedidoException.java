package br.pucminas.aed.vendas.domain;

public class LimiteDeIngressosExcedidoException extends RuntimeException {

    private final int jaReservados;
    private final int pedidos;
    private final int limite;

    public LimiteDeIngressosExcedidoException(int jaReservados, int pedidos, int limite) {
        super("limite de %d ingressos por CPF excedido: %d ja reservados, %d pedidos agora"
                .formatted(limite, jaReservados, pedidos));
        this.jaReservados = jaReservados;
        this.pedidos = pedidos;
        this.limite = limite;
    }

    public int getJaReservados() {
        return jaReservados;
    }

    public int getPedidos() {
        return pedidos;
    }

    public int getLimite() {
        return limite;
    }
}
