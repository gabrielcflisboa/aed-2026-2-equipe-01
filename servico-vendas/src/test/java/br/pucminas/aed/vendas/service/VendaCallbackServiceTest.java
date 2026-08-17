package br.pucminas.aed.vendas.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import br.pucminas.aed.vendas.domain.IngressoReservadoEvent;
import br.pucminas.aed.vendas.domain.ItemDoIngressoVO;

/**
 * Cobre o contrato produzido pela VendaCallbackService: os cinco
 * cabecalhos ce_* do envelope CloudEvents, a chave de particao, e os dois
 * desfechos possiveis do envio.
 *
 * Sem contexto Spring e sem broker: o KafkaTemplate e mockado e o
 * ProducerRecord e interceptado antes de sair. Roda em milissegundos.
 */
@ExtendWith(MockitoExtension.class)
class VendaCallbackServiceTest {

    private static final String TOPICO = "vendas.ingresso.reservado.v1";
    private static final String ORIGEM = "servico-vendas";
    private static final String TIPO = "vendas.ingresso.reservado.v1";

    private static final Instant RESERVADO_EM = Instant.parse("2026-08-14T11:59:29.411Z");

    @Mock
    private KafkaTemplate<String, Object> clienteDoBroker;

    private VendaCallbackService vendaCallbackService;
    private IngressoReservadoEvent evento;

    @BeforeEach
    void preparar() {
        vendaCallbackService = new VendaCallbackService(clienteDoBroker, ORIGEM);
        evento = new IngressoReservadoEvent(
                "b7e1f0c2-0000-4000-8000-000000000001",
                "compra-4711",
                "000.000.000-00",
                "Show da Banda Ficticia",
                List.of(new ItemDoIngressoVO("PISTA", 2, new BigDecimal("180.00"))),
                RESERVADO_EM);
    }

    @Test
    @DisplayName("preenche os cinco cabecalhos ce_* do envelope CloudEvents 1.0")
    void preencheOsCabecalhosCloudEvents() {
        publicacaoBemSucedida();

        ProducerRecord<String, Object> registro = publicarECapturar("PISTA");

        assertThat(cabecalho(registro, "ce_specversion")).isEqualTo("1.0");
        assertThat(cabecalho(registro, "ce_source")).isEqualTo(ORIGEM);
        assertThat(cabecalho(registro, "ce_type")).isEqualTo(TIPO);
        assertThat(cabecalho(registro, "ce_id"))
                .isEqualTo(evento.getEventoId())
                .isNotEqualTo(evento.getCompraId());
    }

    @Test
    @DisplayName("ce_time viaja em ISO-8601, nunca em epoch, e reflete quando o fato ocorreu")
    void ceTimeEmIso8601() {
        publicacaoBemSucedida();

        String ceTime = cabecalho(publicarECapturar("PISTA"), "ce_time");

        assertThat(ceTime).isEqualTo("2026-08-14T11:59:29.411Z");
        assertThat(ceTime).doesNotMatch("-?\\d+");
        assertThat(Instant.parse(ceTime)).isEqualTo(evento.getReservadoEm());
    }

    @Test
    @DisplayName("publica no topico configurado usando a chave de particao recebida")
    void usaAChaveDeParticaoRecebida() {
        publicacaoBemSucedida();

        ProducerRecord<String, Object> registro = publicarECapturar("PISTA");

        assertThat(registro.topic()).isEqualTo(TOPICO);
        assertThat(registro.key()).isEqualTo("PISTA");
        assertThat(registro.value()).isSameAs(evento);
    }

    @Test
    @DisplayName("falha na publicacao e registrada, nao propagada para quem chamou")
    void falhaNaPublicacaoNaoPropaga() {
        when(clienteDoBroker.send(ArgumentMatchers.<ProducerRecord<String, Object>>any()))
                .thenReturn(CompletableFuture.failedFuture(
                        new IllegalStateException("broker indisponivel")));

        assertDoesNotThrow(() -> vendaCallbackService.publicar(TOPICO, "PISTA", evento.getEventoId(),
            evento.getReservadoEm(), TIPO, evento));
    }

    private void publicacaoBemSucedida() {
        RecordMetadata metadados =
                new RecordMetadata(new TopicPartition(TOPICO, 0), 7L, 0, RESERVADO_EM.toEpochMilli(), 5, 120);
        when(clienteDoBroker.send(ArgumentMatchers.<ProducerRecord<String, Object>>any()))
                .thenAnswer(invocacao -> CompletableFuture.completedFuture(
                        new SendResult<>(invocacao.getArgument(0), metadados)));
    }

    private ProducerRecord<String, Object> publicarECapturar(String chaveDeParticao) {
        vendaCallbackService.publicar(TOPICO, chaveDeParticao, evento.getEventoId(),
            evento.getReservadoEm(), TIPO, evento);

        ArgumentCaptor<ProducerRecord<String, Object>> capturado = ArgumentCaptor.captor();
        org.mockito.Mockito.verify(clienteDoBroker).send(capturado.capture());
        return capturado.getValue();
    }

    private static String cabecalho(ProducerRecord<?, ?> registro, String nome) {
        Header header = registro.headers().lastHeader(nome);
        assertThat(header).as("cabecalho %s ausente", nome).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
