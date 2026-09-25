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

import br.pucminas.aed.vendas.domain.IngressoReservaCompensadaEvent;
import br.pucminas.aed.vendas.domain.IngressoReservadoEvent;
import br.pucminas.aed.vendas.domain.ItemDoIngressoVO;

/**
 * Cobre o contrato produzido pela VendaCompensacaoCallbackService: os cinco
 * cabecalhos ce_* do envelope CloudEvents, a chave de particao, e os dois
 * desfechos possiveis do envio. Espelha VendaCallbackServiceTest.
 */
@ExtendWith(MockitoExtension.class)
class VendaCompensacaoCallbackServiceTest {

    private static final String TOPICO = "vendas.ingresso.reserva-compensada.v1";
    private static final String ORIGEM = "servico-vendas";
    private static final String TIPO = "vendas.ingresso.reserva-compensada.v1";

    private static final Instant RESERVADO_EM = Instant.parse("2026-08-14T11:59:29.411Z");
    private static final Instant COMPENSADO_EM = Instant.parse("2026-08-14T12:05:00.000Z");

    @Mock
    private KafkaTemplate<String, IngressoReservaCompensadaEvent> clienteDoBroker;

    private VendaCompensacaoCallbackService vendaCompensacaoCallbackService;
    private IngressoReservaCompensadaEvent evento;

    @BeforeEach
    void preparar() {
        vendaCompensacaoCallbackService = new VendaCompensacaoCallbackService(clienteDoBroker, TOPICO, ORIGEM, TIPO);

        var reserva = new IngressoReservadoEvent(
                "b7e1f0c2-0000-4000-8000-000000000001",
                "compra-4711",
                "000.000.000-00",
                "Show da Banda Ficticia",
                List.of(new ItemDoIngressoVO("PISTA", 2, new BigDecimal("180.00"))),
                RESERVADO_EM);

        evento = new IngressoReservaCompensadaEvent(
                "c9e2f0c2-0000-4000-8000-000000000002",
                reserva.getCompraId(),
                reserva.getEvento(),
                reserva.getItens(),
                "pagamento recusado pelo gateway (simulado)",
                COMPENSADO_EM);
    }

    @Test
    @DisplayName("preenche os cinco cabecalhos ce_* do envelope CloudEvents 1.0")
    void preencheOsCabecalhosCloudEvents() {
        publicacaoBemSucedida();

        ProducerRecord<String, IngressoReservaCompensadaEvent> registro = publicarECapturar("Show da Banda Ficticia");

        assertThat(cabecalho(registro, "ce_specversion")).isEqualTo("1.0");
        assertThat(cabecalho(registro, "ce_source")).isEqualTo(ORIGEM);
        assertThat(cabecalho(registro, "ce_type")).isEqualTo(TIPO);
        assertThat(cabecalho(registro, "ce_id"))
                .isEqualTo(evento.getEventoId())
                .isNotEqualTo(evento.getCompraId());
    }

    @Test
    @DisplayName("ce_time viaja em ISO-8601, nunca em epoch, e reflete quando a compensacao ocorreu")
    void ceTimeEmIso8601() {
        publicacaoBemSucedida();

        String ceTime = cabecalho(publicarECapturar("Show da Banda Ficticia"), "ce_time");

        assertThat(ceTime).isEqualTo("2026-08-14T12:05:00Z");
        assertThat(ceTime).doesNotMatch("-?\\d+");
        assertThat(Instant.parse(ceTime)).isEqualTo(evento.getCompensadoEm());
    }

    @Test
    @DisplayName("publica no topico configurado usando a chave de particao recebida")
    void usaAChaveDeParticaoRecebida() {
        publicacaoBemSucedida();

        ProducerRecord<String, IngressoReservaCompensadaEvent> registro = publicarECapturar("Show da Banda Ficticia");

        assertThat(registro.topic()).isEqualTo(TOPICO);
        assertThat(registro.key()).isEqualTo("Show da Banda Ficticia");
        assertThat(registro.value()).isSameAs(evento);
    }

    @Test
    @DisplayName("falha na publicacao e registrada, nao propagada para quem chamou")
    void falhaNaPublicacaoNaoPropaga() {
        when(clienteDoBroker.send(ArgumentMatchers.<ProducerRecord<String, IngressoReservaCompensadaEvent>>any()))
                .thenReturn(CompletableFuture.failedFuture(
                        new IllegalStateException("broker indisponivel")));

        assertDoesNotThrow(() -> vendaCompensacaoCallbackService.publicar(evento, "Show da Banda Ficticia"));
    }

    private void publicacaoBemSucedida() {
        RecordMetadata metadados =
                new RecordMetadata(new TopicPartition(TOPICO, 0), 7L, 0, COMPENSADO_EM.toEpochMilli(), 5, 120);
        when(clienteDoBroker.send(ArgumentMatchers.<ProducerRecord<String, IngressoReservaCompensadaEvent>>any()))
                .thenAnswer(invocacao -> CompletableFuture.completedFuture(
                        new SendResult<>(invocacao.getArgument(0), metadados)));
    }

    private ProducerRecord<String, IngressoReservaCompensadaEvent> publicarECapturar(String chaveDeParticao) {
        vendaCompensacaoCallbackService.publicar(evento, chaveDeParticao);

        ArgumentCaptor<ProducerRecord<String, IngressoReservaCompensadaEvent>> capturado = ArgumentCaptor.captor();
        org.mockito.Mockito.verify(clienteDoBroker).send(capturado.capture());
        return capturado.getValue();
    }

    private static String cabecalho(ProducerRecord<?, ?> registro, String nome) {
        Header header = registro.headers().lastHeader(nome);
        assertThat(header).as("cabecalho %s ausente", nome).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
