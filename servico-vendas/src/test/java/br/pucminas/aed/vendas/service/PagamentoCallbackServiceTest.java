package br.pucminas.aed.vendas.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import br.pucminas.aed.vendas.domain.PagamentoRecusadoEvent;

class PagamentoCallbackServiceTest {

    private static final String TOPICO = "pagamentos.pagamento.recusado.v1";
    private static final String ORIGEM = "gateway-de-pagamento-simulado";
    private static final String TIPO = "pagamentos.pagamento.recusado.v1";
    private static final Instant RECUSADO_EM =
            Instant.parse("2026-08-14T12:05:00Z");

    @Mock
    private KafkaTemplate<String, PagamentoRecusadoEvent> clienteDoBroker;

    private PagamentoCallbackService pagamentoCallbackService;

    private PagamentoRecusadoEvent evento;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        pagamentoCallbackService = new PagamentoCallbackService(
                clienteDoBroker,
                TOPICO,
                ORIGEM,
                TIPO);

        evento = new PagamentoRecusadoEvent(
                "c8e2f0c2-0000-4000-8000-000000000001",
                "compra-4711",
                "Pagamento recusado pelo gateway",
                RECUSADO_EM);
    }

    @Test
    void devePublicarComOsCincoHeadersCloudEvents() {
        when(clienteDoBroker.send(
                ArgumentMatchers.<ProducerRecord<String, PagamentoRecusadoEvent>>any()))
                .thenReturn(publicacaoBemSucedida());

        ProducerRecord<String, PagamentoRecusadoEvent> registro =
                publicarECapturar();

        assertThat(cabecalho(registro, "ce_specversion"))
                .isEqualTo("1.0");

        assertThat(cabecalho(registro, "ce_id"))
                .isEqualTo(evento.getEventoId())
                .isNotEqualTo(evento.getCompraId());

        assertThat(cabecalho(registro, "ce_source"))
                .isEqualTo(ORIGEM);

        assertThat(cabecalho(registro, "ce_type"))
                .isEqualTo(TIPO);
    }

    @Test
    void devePublicarCeTimeComRecusadoEmEmFormatoIso() {
        when(clienteDoBroker.send(
                ArgumentMatchers.<ProducerRecord<String, PagamentoRecusadoEvent>>any()))
                .thenReturn(publicacaoBemSucedida());

        ProducerRecord<String, PagamentoRecusadoEvent> registro =
                publicarECapturar();

        assertThat(cabecalho(registro, "ce_time"))
                .isEqualTo("2026-08-14T12:05:00Z");

        assertThat(Instant.parse(cabecalho(registro, "ce_time")))
                .isEqualTo(evento.getRecusadoEm());
    }

    @Test
    void devePublicarNoTopicoComCompraIdComoChave() {
        when(clienteDoBroker.send(
                ArgumentMatchers.<ProducerRecord<String, PagamentoRecusadoEvent>>any()))
                .thenReturn(publicacaoBemSucedida());

        ProducerRecord<String, PagamentoRecusadoEvent> registro =
                publicarECapturar();

        assertThat(registro.topic())
                .isEqualTo(TOPICO);

        assertThat(registro.key())
                .isEqualTo(evento.getCompraId());

        assertThat(registro.value())
                .isSameAs(evento);
    }

    @Test
    void falhaDoBrokerNaoDeveSerPropagada() {
        when(clienteDoBroker.send(
                ArgumentMatchers.<ProducerRecord<String, PagamentoRecusadoEvent>>any()))
                .thenReturn(CompletableFuture.failedFuture(
                        new IllegalStateException("broker indisponivel")));

        assertDoesNotThrow(() ->
                pagamentoCallbackService.publicar(evento));
    }

    private CompletableFuture<SendResult<String, PagamentoRecusadoEvent>>
            publicacaoBemSucedida() {

        RecordMetadata metadados = new RecordMetadata(
                new TopicPartition(TOPICO, 0),
                7L,
                0,
                RECUSADO_EM.toEpochMilli(),
                5,
                120);

        ProducerRecord<String, PagamentoRecusadoEvent> registro =
                new ProducerRecord<>(TOPICO, evento.getCompraId(), evento);

        return CompletableFuture.completedFuture(
                new SendResult<>(registro, metadados));
    }

    private ProducerRecord<String, PagamentoRecusadoEvent>
            publicarECapturar() {

        pagamentoCallbackService.publicar(evento);

        ArgumentCaptor<ProducerRecord<String, PagamentoRecusadoEvent>> capturado =
                ArgumentCaptor.forClass(ProducerRecord.class);

        verify(clienteDoBroker).send(capturado.capture());

        return capturado.getValue();
    }

    private String cabecalho(
            ProducerRecord<String, PagamentoRecusadoEvent> registro,
            String nome) {

        return new String(
                registro.headers().lastHeader(nome).value(),
                java.nio.charset.StandardCharsets.UTF_8);
    }
}