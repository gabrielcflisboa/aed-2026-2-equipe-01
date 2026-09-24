package br.pucminas.aed.vendas.service;

import org.springframework.stereotype.Service;

/**
 * Simula um gateway de pagamento externo.
 *
 * Um gateway real e um sistema de terceiro: recebe a cobranca, decide aprovar
 * ou recusar, e avisa o resultado de volta (tipicamente por webhook), de forma
 * assincrona e fora do controle deste servico. Essa classe existe pra isolar
 * essa decisao — mesmo simulada — da logica de negocio do servico-vendas: o
 * VendaService nao decide recusar pagamento, ele so reage a uma recusa que ja
 * aconteceu em outro lugar.
 *
 * Nesta simulacao, quem faz o papel do webhook e o proprio endpoint
 * POST /vendas/reservas/{compraId}/compensacoes: uma chamada manual representa
 * o gateway avisando que recusou o pagamento daquela compra.
 */
@Service
public class GatewayDePagamentoService {

    /**
     * Simula a recusa do pagamento de uma compra e devolve o motivo.
     *
     * @param compraId a compra cujo pagamento foi recusado
     * @return o motivo da recusa
     */
    public String recusar(String compraId) {
        return "pagamento recusado pelo gateway (simulado)";
    }
}
