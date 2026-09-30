package br.pucminas.aed.vendas.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import br.pucminas.aed.vendas.domain.PagamentoRecusadoEvent;
import br.pucminas.aed.vendas.service.VendaService;
import tools.jackson.databind.ObjectMapper;

@Component
public class PagamentoListener {

    private static final Logger logger = LoggerFactory.getLogger(PagamentoListener.class);

    private final VendaService vendaService;
    private final ObjectMapper conversorJson = new ObjectMapper();

    public PagamentoListener(VendaService vendaService) {
        this.vendaService = vendaService;
    }

    @KafkaListener(topics = "${app.topico-pagamentos-recusados}", groupId = "${spring.kafka.consumer.group-id}")
    public void receberRecusa(ConsumerRecord<String, String> registro, Acknowledgment ack) {
        var recusa = conversorJson.readValue(registro.value(), PagamentoRecusadoEvent.class);
        logger.info("pagamento recusado recebido: eventoId={} compraId={} particao={} offset={}",
                recusa.getEventoId(), recusa.getCompraId(), registro.partition(), registro.offset());

        vendaService.liberarReserva(recusa).ifPresentOrElse(
                liberacao -> logger.info("reserva liberada: compraId={} reservaEventoId={}",
                        liberacao.getCompraId(), liberacao.getReservaEventoId()),
                () -> logger.info("recusa repetida: compraId={} ja liberada", recusa.getCompraId()));

        ack.acknowledge();
    }
}