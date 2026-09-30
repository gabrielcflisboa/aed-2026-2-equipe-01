package br.pucminas.aed.ingressos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import br.pucminas.aed.ingressos.domain.EventoDoEstoqueRepository;
import br.pucminas.aed.ingressos.domain.IngressoDevolvidoEvent;
import br.pucminas.aed.ingressos.domain.IngressoRetiradoEvent;
import br.pucminas.aed.ingressos.domain.PedidoDeReprocessamentoVO;
import br.pucminas.aed.ingressos.domain.StreamDoEstoqueVO;
import br.pucminas.aed.ingressos.service.AberturaDeSetoresService;
import br.pucminas.aed.ingressos.service.ReprocessamentoService;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:liberacao-que-falha;DB_CLOSE_DELAY=-1",
        "spring.kafka.listener.auto-startup=true",
        "spring.kafka.admin.auto-create=true",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "app.resiliencia.retentativas=3",
        "app.resiliencia.espera-inicial-ms=500",
        "app.resiliencia.espera-maxima-ms=2000"
})
@EmbeddedKafka(partitions = 1, bootstrapServersProperty = "spring.kafka.bootstrap-servers", topics = {
        "vendas.ingresso.reservado.v1",
        "vendas.ingresso.liberado.v1",
        "vendas.ingresso.reservado.v1.dlq",
        "vendas.ingresso.liberado.v1.dlq"
})
@DirtiesContext
class LiberacaoQueFalhaTest {

    private static final String TOPICO_RESERVAS = "vendas.ingresso.reservado.v1";
    private static final String TOPICO_LIBERACOES = "vendas.ingresso.liberado.v1";
    private static final String TOPICO_LIBERACOES_DLQ = "vendas.ingresso.liberado.v1.dlq";

    @Autowired
    private KafkaTemplate<String, String> clienteDoBroker;

    @Autowired
    private AberturaDeSetoresService aberturaDeSetoresService;

    @Autowired
    private EventoDoEstoqueRepository eventoDoEstoqueRepository;

    @Autowired
    private ReprocessamentoService reprocessamentoService;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void liberacaoAntesDaReservaPassaEmUmaRetentativaSemIrParaDlq() throws Exception {
        String evento = "SHOW-LIBERACAO-FALHA-1";
        String setor = "PISTA";

        aberturaDeSetoresService.abrir(evento, setor, 10);

        UUID reservaEventoId = UUID.randomUUID();
        UUID liberacaoEventoId = UUID.randomUUID();

        String liberacaoJson = """
                {
                  "eventoId": "%s",
                  "compraId": "compra-liberacao-falha-1",
                  "reservaEventoId": "%s",
                  "pagamentoEventoId": "%s",
                  "evento": "%s",
                  "itens": [
                    {
                      "setor": "%s",
                      "quantidade": 2
                    }
                  ],
                  "motivo": "PAGAMENTO_RECUSADO",
                  "liberadoEm": "%s"
                }
                """.formatted(
                liberacaoEventoId,
                reservaEventoId,
                UUID.randomUUID(),
                evento,
                setor,
                Instant.now());

        String reservaJson = """
                {
                  "eventoId": "%s",
                  "cpfComprador": "12345678900",
                  "evento": "%s",
                  "itens": [
                    {
                      "setor": "%s",
                      "quantidade": 2
                    }
                  ],
                  "reservadoEm": "%s"
                }
                """.formatted(
                reservaEventoId,
                evento,
                setor,
                Instant.now());

        publicar(
                TOPICO_LIBERACOES,
                liberacaoEventoId.toString(),
                TOPICO_LIBERACOES,
                evento,
                liberacaoJson);

        Thread.sleep(700);

        publicar(
                TOPICO_RESERVAS,
                reservaEventoId.toString(),
                TOPICO_RESERVAS,
                evento,
                reservaJson);

        StreamDoEstoqueVO stream = StreamDoEstoqueVO.de(evento, setor);

        await()
                .atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> {
                    var log = eventoDoEstoqueRepository.lerStream(stream);

                    assertThat(log)
                            .anySatisfy(eventoGravado ->
                                    assertThat(eventoGravado.getEvento())
                                            .isInstanceOf(IngressoRetiradoEvent.class));

                    assertThat(log)
                            .anySatisfy(eventoGravado -> {
                                assertThat(eventoGravado.getEvento())
                                        .isInstanceOf(IngressoDevolvidoEvent.class);

                                IngressoDevolvidoEvent devolvido =
                                        (IngressoDevolvidoEvent) eventoGravado.getEvento();

                                assertThat(devolvido.getReservaEventoId())
                                        .isEqualTo(reservaEventoId.toString());
                            });
                });

        assertThat(dlqContemCeId(liberacaoEventoId.toString())).isFalse();
    }

    @Test
    void liberacaoSemReservaVaiParaDlqEPodeSerReprocessadaSemDuplicar() throws Exception {
        String evento = "SHOW-LIBERACAO-FALHA-2";
        String setor = "PISTA";

        aberturaDeSetoresService.abrir(evento, setor, 10);

        UUID reservaEventoId = UUID.randomUUID();
        UUID liberacaoEventoId = UUID.randomUUID();

        String liberacaoJson = """
                {
                  "eventoId": "%s",
                  "compraId": "compra-liberacao-falha-2",
                  "reservaEventoId": "%s",
                  "pagamentoEventoId": "%s",
                  "evento": "%s",
                  "itens": [
                    {
                      "setor": "%s",
                      "quantidade": 2
                    }
                  ],
                  "motivo": "PAGAMENTO_RECUSADO",
                  "liberadoEm": "%s"
                }
                """.formatted(
                liberacaoEventoId,
                reservaEventoId,
                UUID.randomUUID(),
                evento,
                setor,
                Instant.now());

        publicar(
                TOPICO_LIBERACOES,
                liberacaoEventoId.toString(),
                TOPICO_LIBERACOES,
                evento,
                liberacaoJson);

        await()
                .atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> {
                    ConsumerRecord<String, String> registro =
                            buscarNaDlq(liberacaoEventoId.toString());

                    assertThat(registro).isNotNull();

                    assertThat(header(registro, "dlq_classificacao"))
                            .isEqualTo("transitoria-esgotada");

                    assertThat(header(registro, "dlq_excecao"))
                            .endsWith("ReservaAindaNaoProcessadaException");
                });

        String reservaJson = """
                {
                  "eventoId": "%s",
                  "cpfComprador": "12345678901",
                  "evento": "%s",
                  "itens": [
                    {
                      "setor": "%s",
                      "quantidade": 2
                    }
                  ],
                  "reservadoEm": "%s"
                }
                """.formatted(
                reservaEventoId,
                evento,
                setor,
                Instant.now());

        publicar(
                TOPICO_RESERVAS,
                reservaEventoId.toString(),
                TOPICO_RESERVAS,
                evento,
                reservaJson);

        StreamDoEstoqueVO stream = StreamDoEstoqueVO.de(evento, setor);

        await()
                .atMost(Duration.ofSeconds(20))
                .untilAsserted(() ->
                        assertThat(eventoDoEstoqueRepository.lerStream(stream))
                                .anySatisfy(eventoGravado ->
                                        assertThat(eventoGravado.getEvento())
                                                .isInstanceOf(IngressoRetiradoEvent.class)));

        reprocessamentoService.reprocessar(
                new PedidoDeReprocessamentoVO(
                        TOPICO_LIBERACOES_DLQ,
                        List.of(liberacaoEventoId.toString())));

        await()
                .atMost(Duration.ofSeconds(20))
                .untilAsserted(() ->
                        assertThat(contarDevolucoes(stream, reservaEventoId))
                                .isEqualTo(1));

        reprocessamentoService.reprocessar(
                new PedidoDeReprocessamentoVO(
                        TOPICO_LIBERACOES_DLQ,
                        List.of(liberacaoEventoId.toString())));

        await()
                .during(Duration.ofSeconds(2))
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() ->
                        assertThat(contarDevolucoes(stream, reservaEventoId))
                                .isEqualTo(1));
    }

    private long contarDevolucoes(
            StreamDoEstoqueVO stream,
            UUID reservaEventoId) {

        return eventoDoEstoqueRepository.lerStream(stream).stream()
                .map(eventoGravado -> eventoGravado.getEvento())
                .filter(IngressoDevolvidoEvent.class::isInstance)
                .map(IngressoDevolvidoEvent.class::cast)
                .filter(evento ->
                        reservaEventoId.toString()
                                .equals(evento.getReservaEventoId()))
                .count();
    }

    private boolean dlqContemCeId(String ceId) {
        return buscarNaDlq(ceId) != null;
    }

    private ConsumerRecord<String, String> buscarNaDlq(String ceId) {
        Map<String, Object> propriedades =
                KafkaTestUtils.consumerProps(
                        embeddedKafka,
                        "teste-liberacao-dlq-" + UUID.randomUUID(),
                        false);

        propriedades.put("auto.offset.reset", "earliest");

        try (Consumer<String, String> consumidor =
                     new DefaultKafkaConsumerFactory<>(
                             propriedades,
                             new StringDeserializer(),
                             new StringDeserializer())
                             .createConsumer()) {

            embeddedKafka.consumeFromAnEmbeddedTopic(
                    consumidor,
                    TOPICO_LIBERACOES_DLQ);

            ConsumerRecords<String, String> registros =
                    KafkaTestUtils.getRecords(
                            consumidor,
                            Duration.ofSeconds(3));

            for (ConsumerRecord<String, String> registro : registros) {
                if (ceId.equals(header(registro, "ce_id"))) {
                    return registro;
                }
            }

            return null;
        }
    }

    private String header(
            ConsumerRecord<String, String> registro,
            String nome) {

        var cabecalho = registro.headers().lastHeader(nome);

        if (cabecalho == null) {
            return null;
        }

        return new String(
                cabecalho.value(),
                StandardCharsets.UTF_8);
    }

    private void publicar(
            String topico,
            String ceId,
            String ceType,
            String chave,
            String json) {

        var registro =
                new ProducerRecord<String, String>(
                        topico,
                        chave,
                        json);

        registro.headers()
                .add(
                        "ce_specversion",
                        "1.0".getBytes(StandardCharsets.UTF_8))
                .add(
                        "ce_id",
                        ceId.getBytes(StandardCharsets.UTF_8))
                .add(
                        "ce_source",
                        "teste".getBytes(StandardCharsets.UTF_8))
                .add(
                        "ce_type",
                        ceType.getBytes(StandardCharsets.UTF_8))
                .add(
                        "ce_time",
                        Instant.now()
                                .toString()
                                .getBytes(StandardCharsets.UTF_8));

        clienteDoBroker.send(registro).join();
    }
}