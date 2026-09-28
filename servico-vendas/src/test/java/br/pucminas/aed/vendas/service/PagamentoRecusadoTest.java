package br.pucminas.aed.vendas.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest(properties = {
        "spring.kafka.admin.auto-create=true",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "app.resiliencia.retentativas=1",
        "app.resiliencia.espera-inicial-ms=100",
        "app.resiliencia.espera-maxima-ms=100"
})
@EmbeddedKafka(
        partitions = 1,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        topics = {
                "vendas.ingresso.reservado.v1",
                "vendas.ingresso.liberado.v1",
                "pagamentos.pagamento.recusado.v1",
                "pagamentos.pagamento.recusado.v1.dlq"
        })
@DirtiesContext
class PagamentoRecusadoTest {

    private static final String TOPICO_PAGAMENTOS_RECUSADOS =
            "pagamentos.pagamento.recusado.v1";

    @Autowired
    private GatewayDePagamentoService gatewayDePagamentoService;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void devePublicarPagamentoRecusadoNoKafka() {

        try (Consumer<String, String> consumer =
                leitor("teste-pagamento-recusado",
                        TOPICO_PAGAMENTOS_RECUSADOS)) {

            var recusa =
                    gatewayDePagamentoService.recusar("compra-0001");

            ConsumerRecord<String, String> registro =
                    KafkaTestUtils.getSingleRecord(
                            consumer,
                            TOPICO_PAGAMENTOS_RECUSADOS,
                            Duration.ofSeconds(10));

            assertThat(registro).isNotNull();

            assertThat(registro.key())
                    .isEqualTo("compra-0001");

            assertThat(registro.value())
                    .contains(recusa.getEventoId());

            assertThat(registro.value())
                    .contains(recusa.getCompraId());

            assertThat(registro.value())
                    .contains(recusa.getMotivo());
        }
    }

    private Consumer<String, String> leitor(
            String grupo,
            String topico) {

        Map<String, Object> propriedades =
                KafkaTestUtils.consumerProps(
                        embeddedKafka,
                        grupo,
                        false);

        propriedades.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");

        Consumer<String, String> consumer =
                new DefaultKafkaConsumerFactory<>(
                        propriedades,
                        new StringDeserializer(),
                        new StringDeserializer())
                        .createConsumer();

        embeddedKafka.consumeFromAnEmbeddedTopic(
                consumer,
                topico);

        return consumer;
    }
}