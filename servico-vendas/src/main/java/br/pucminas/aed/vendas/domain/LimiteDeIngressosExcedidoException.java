package br.pucminas.aed.vendas.domain;

/**
 * A reserva foi recusada porque o comprador passou do limite de ingressos.
 *
 * E o ponto de decisao que o ADR-002 promete: sem esta excecao, o publisher
 * seria um repassador de mensagens, nao um servico com regra de negocio.
 * Nao carrega o CPF de proposito — o motivo da recusa nao precisa devolver
 * dado pessoal na resposta HTTP.
 */
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
