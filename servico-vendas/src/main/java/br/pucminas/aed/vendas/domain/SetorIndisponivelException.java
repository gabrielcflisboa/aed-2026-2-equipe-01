package br.pucminas.aed.vendas.domain;

/**
 * A reserva foi recusada porque o setor nao existe no evento ou nao comporta
 * a quantidade pedida.
 *
 * O publisher so conhece a capacidade declarada do setor, nao o que ja foi
 * vendido — quem tem essa verdade e o servico-ingressos. Aqui a checagem
 * serve para barrar cedo o pedido que nunca poderia caber.
 */
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
