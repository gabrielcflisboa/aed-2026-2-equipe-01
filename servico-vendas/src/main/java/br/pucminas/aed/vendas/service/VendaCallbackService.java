package br.pucminas.aed.vendas.service;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import br.pucminas.aed.vendas.domain.IngressoReservadoEvent;

@Service
public class VendaCallbackService {

    private static final Logger log = LoggerFactory.getLogger(VendaCallbackService.class);

    public void registrar(CompletableFuture<SendResult<String, IngressoReservadoEvent>> envio, String eventoId) {
        envio.whenComplete((resultado, falha) -> {
            if (falha != null) {
                log.error("falha ao publicar eventoId={}", eventoId, falha);
                return;
            }
            var destino = resultado.getRecordMetadata();
            log.info("publicado eventoId={} topico={} particao={} offset={}",
                    eventoId, destino.topic(), destino.partition(), destino.offset());
        });
    }
}
