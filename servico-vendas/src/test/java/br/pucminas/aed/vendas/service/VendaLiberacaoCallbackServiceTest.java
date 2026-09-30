package br.pucminas.aed.vendas.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import br.pucminas.aed.vendas.domain.IngressoLiberadoEvent;
import br.pucminas.aed.vendas.domain.ItemDoIngressoVO;

@ExtendWith(MockitoExtension.class)
class VendaLiberacaoCallbackServiceTest {

    private static final String TOPICO = "vendas.ingresso.liberado.v1";
    private static final String ORIGEM = "servico-vendas";
    private static final String TIPO = "vendas.ingresso.liberado.v1";
    private static final Instant LIBERADO_EM = Instant.parse("2026-08-14T12:05:00.000Z");

    @Mock
    private KafkaTemplate<String, IngressoLiberadoEvent> clienteDoBroker;

    private VendaLiberacaoCallbackService vendaLiberacaoCallbackService;
    private IngressoLiberadoEvent evento;

    @BeforeEach
    void preparar() {
        vendaLiberacaoCallbackService = new VendaLiberacaoCallbackService(clienteDoBroker, TOPICO, ORIGEM, TIPO, 1000L);
        evento = new IngressoLiberadoEvent(
                "c9e2f0c2-0000-4000-8000-000000000002",
                "compra-4711",
                "b7e1f0c2-0000-4000-8000-000000000001",
                "d1e2f0c2-0000-4000-8000-000000000003",
                "Show da Banda Ficticia",
                List.of(new ItemDoIngressoVO("PISTA", 2, new BigDecimal("180.00"))),
                "pagamento recusado pelo gateway (simulado)",
                LIBERADO_EM);
    }

    @Test
    @DisplayName("preenche os cinco cabecalhos ce_* e usa liberadoEm como ce_time")
    void preencheOsCabecalhosCloudEvents() {
        publicacaoBemSucedida();

        var registro = publicarECapturar();

        assertThat(cabecalho(registro, "ce_specversion")).isEqualTo("1.0");
        assertThat(cabecalho(registro, "ce_source")).isEqualTo(ORIGEM);
        assertThat(cabecalho(registro, "ce_type")).isEqualTo(TIPO);
        assertThat(cabecalho(registro, "ce_id")).isEqualTo(evento.getEventoId());
        assertThat(cabecalho(registro, "ce_time")).isEqualTo("2026-08-14T12:05:00Z");
        assertThat(registro.topic()).isEqualTo(TOPICO);
        assertThat(registro.key()).isEqualTo("Show da Banda Ficticia");
    }

    @Test
    @DisplayName("sem confirmacao do broker, a publicacao lanca KafkaException para o listener retentar")
    void falhaDoBrokerViraExcecao() {
        when(clienteDoBroker.send(ArgumentMatchers.<ProducerRecord<String, IngressoLiberadoEvent>>any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker indisponivel")));

        assertThatThrownBy(() -> vendaLiberacaoCallbackService.publicar(evento, "Show da Banda Ficticia"))
                .isInstanceOf(KafkaException.class);
    }

    private void publicacaoBemSucedida() {
        RecordMetadata metadados =
                new RecordMetadata(new TopicPartition(TOPICO, 0), 7L, 0, LIBERADO_EM.toEpochMilli(), 5, 120);
        when(clienteDoBroker.send(ArgumentMatchers.<ProducerRecord<String, IngressoLiberadoEvent>>any()))
                .thenAnswer(invocacao -> CompletableFuture.completedFuture(
                        new SendResult<>(invocacao.getArgument(0), metadados)));
    }

    private ProducerRecord<String, IngressoLiberadoEvent> publicarECapturar() {
        vendaLiberacaoCallbackService.publicar(evento, "Show da Banda Ficticia");

        ArgumentCaptor<ProducerRecord<String, IngressoLiberadoEvent>> capturado = ArgumentCaptor.captor();
        verify(clienteDoBroker).send(capturado.capture());
        return capturado.getValue();
    }

    private static String cabecalho(ProducerRecord<?, ?> registro, String nome) {
        Header header = registro.headers().lastHeader(nome);
        assertThat(header).as("cabecalho %s ausente", nome).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}