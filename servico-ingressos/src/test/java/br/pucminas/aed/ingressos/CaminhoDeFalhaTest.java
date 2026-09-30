package br.pucminas.aed.ingressos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import br.pucminas.aed.ingressos.domain.AgregacaoDeSetorVO;
import br.pucminas.aed.ingressos.domain.DeduplicacaoRepository;
import br.pucminas.aed.ingressos.domain.EventoDoEstoqueRepository;
import br.pucminas.aed.ingressos.domain.EventoGravadoVO;
import br.pucminas.aed.ingressos.domain.IngressoRetiradoEvent;
import br.pucminas.aed.ingressos.domain.PedidoDeReprocessamentoVO;
import br.pucminas.aed.ingressos.domain.StreamDoEstoqueVO;
import br.pucminas.aed.ingressos.service.AberturaDeSetoresService;
import br.pucminas.aed.ingressos.service.AgregacaoDeReservasService;
import br.pucminas.aed.ingressos.service.ReprocessamentoService;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:caminho-de-falha;DB_CLOSE_DELAY=-1",
        "spring.kafka.listener.auto-startup=true",
        "spring.kafka.admin.auto-create=true",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
@EmbeddedKafka(partitions = 1, bootstrapServersProperty = "spring.kafka.bootstrap-servers", topics = {
        "vendas.ingresso.reservado.v1", "vendas.ingresso.liberado.v1",
        "vendas.ingresso.reservado.v1.dlq", "vendas.ingresso.liberado.v1.dlq"})
@DirtiesContext
class CaminhoDeFalhaTest {

    private static final String TOPICO_RESERVAS = "vendas.ingresso.reservado.v1";
    private static final String DLQ_RESERVAS = "vendas.ingresso.reservado.v1.dlq";
    private static final String GRUPO_DO_ESTOQUE = "servico-ingressos-teste";

    @Autowired
    private KafkaTemplate<String, String> clienteDoBroker;

    @Autowired
    private AberturaDeSetoresService aberturaDeSetoresService;

    @Autowired
    private EventoDoEstoqueRepository eventoDoEstoqueRepository;

    @Autowired
    private AgregacaoDeReservasService agregacaoDeReservasService;

    @Autowired
    private ReprocessamentoService reprocessamentoService;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @MockitoSpyBean
    private DeduplicacaoRepository deduplicacaoRepository;

    @AfterEach
    void desligarFalhaSimulada() {
        Mockito.reset(deduplicacaoRepository);
    }

    @Test
    @DisplayName("JSON invalido vai direto para a DLQ com os ce_* originais e o motivo, sem travar a particao")
    void jsonInvalidoVaiParaADlqSemTravarAParticao() {
        aberturaDeSetoresService.abrir("SHOW-FALHA-1", "PISTA", 10);
        var ceIdInvalido = UUID.randomUUID().toString();
        var ceIdValido = UUID.randomUUID().toString();

        publicar(ceIdInvalido, "show-falha-1", "{ isto nao e json");
        publicar(ceIdValido, "show-falha-1", reserva(ceIdValido, "show-falha-1", 2));

        var retido = esperarNaDlq(ceIdInvalido);
        assertThat(texto(retido, "ce_specversion")).isEqualTo("1.0");
        assertThat(texto(retido, "ce_source")).isEqualTo("caminho-de-falha-test");
        assertThat(texto(retido, "ce_type")).isEqualTo(TOPICO_RESERVAS);
        assertThat(texto(retido, "ce_time")).isNotBlank();
        assertThat(texto(retido, "dlq_classificacao")).isEqualTo("permanente");
        assertThat(texto(retido, "dlq_excecao")).startsWith("tools.jackson");
        assertThat(texto(retido, "dlq_servico")).isEqualTo("servico-ingressos");
        assertThat(Instant.parse(texto(retido, "dlq_falhou_em"))).isBeforeOrEqualTo(Instant.now());
        assertThat(texto(retido, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(TOPICO_RESERVAS);
        assertThat(retido.value()).isEqualTo("{ isto nao e json");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(origensDasRetiradas("SHOW-FALHA-1")).contains(ceIdValido));
    }

    @Test
    @DisplayName("quantidade zero e falha permanente: vai para a DLQ sem retentar")
    void quantidadeZeroEPermanente() {
        var ceId = UUID.randomUUID().toString();

        publicar(ceId, "show-falha-2", reserva(ceId, "show-falha-2", 0));

        var retido = esperarNaDlq(ceId);
        assertThat(texto(retido, "dlq_classificacao")).isEqualTo("permanente");
        assertThat(texto(retido, "dlq_mensagem")).contains("quantidade deve ser maior que zero");
    }

    @Test
    @DisplayName("falha transitoria e retentada e, quando passa, nao chega a DLQ")
    void falhaTransitoriaERetentada() {
        aberturaDeSetoresService.abrir("SHOW-FALHA-3", "PISTA", 10);
        var jaFalhou = new AtomicBoolean(false);
        doAnswer(chamada -> {
            if (!jaFalhou.getAndSet(true)) {
                throw new CannotGetJdbcConnectionException("banco fora, simulado");
            }
            return chamada.callRealMethod();
        }).when(deduplicacaoRepository).registrar(any());
        var ceId = UUID.randomUUID().toString();

        publicar(ceId, "show-falha-3", reserva(ceId, "show-falha-3", 2));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(origensDasRetiradas("SHOW-FALHA-3")).containsExactly(ceId));
        assertThat(jaFalhou).isTrue();
        assertThat(lerDlq()).noneMatch(registro -> ceId.equals(texto(registro, "ce_id")));
    }

    @Test
    @DisplayName("reprocessar duas vezes o mesmo evento da DLQ produz o efeito uma vez, no estoque e no agregador")
    void reprocessarDuasVezesNaoDuplicaOEfeito() {
        aberturaDeSetoresService.abrir("SHOW-FALHA-4", "PISTA", 10);
        var bancoFora = new AtomicBoolean(true);
        doAnswer(chamada -> {
            if (bancoFora.get()) {
                throw new CannotGetJdbcConnectionException("banco fora, simulado");
            }
            return chamada.callRealMethod();
        }).when(deduplicacaoRepository).registrar(any());
        var ceId = UUID.randomUUID().toString();

        publicar(ceId, "show-falha-4", reserva(ceId, "show-falha-4", 2));

        var retido = esperarNaDlq(ceId);
        assertThat(texto(retido, "dlq_classificacao")).isEqualTo("transitoria-esgotada");
        assertThat(texto(retido, "dlq_excecao")).isEqualTo(CannotGetJdbcConnectionException.class.getName());
        bancoFora.set(false);

        var pedido = new PedidoDeReprocessamentoVO(DLQ_RESERVAS, List.of(ceId));
        assertThat(reprocessamentoService.reprocessar(pedido).getRepublicados()).containsExactly(ceId);
        assertThat(reprocessamentoService.reprocessar(pedido).getRepublicados()).containsExactly(ceId);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(origensDasRetiradas("SHOW-FALHA-4")).containsExactly(ceId));
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(origensDasRetiradas("SHOW-FALHA-4")).containsExactly(ceId);
            assertThat(totalAgregado("show-falha-4")).isEqualTo(2);
        });
    }

    private void publicar(String ceId, String chave, String valor) {
        var registro = new ProducerRecord<String, String>(TOPICO_RESERVAS, chave, valor);
        registro.headers()
                .add("ce_specversion", bytes("1.0"))
                .add("ce_id", bytes(ceId))
                .add("ce_source", bytes("caminho-de-falha-test"))
                .add("ce_type", bytes(TOPICO_RESERVAS))
                .add("ce_time", bytes(Instant.now().toString()));
        try {
            clienteDoBroker.send(registro).get(10, TimeUnit.SECONDS);
        } catch (Exception semConfirmacao) {
            throw new IllegalStateException("o broker embutido nao confirmou a publicacao", semConfirmacao);
        }
    }

    private static String reserva(String eventoId, String evento, int quantidade) {
        return """
                {"eventoId":"%s","compraId":"compra-teste","cpfComprador":"000.000.000-00","evento":"%s",\
                "itens":[{"setor":"PISTA","quantidade":%d,"precoUnitario":180.00}],\
                "reservadoEm":"%s"}""".formatted(eventoId, evento, quantidade, Instant.now());
    }

    private ConsumerRecord<String, String> esperarNaDlq(String ceId) {
        try (var leitor = leitor()) {
            var encontrado = new AtomicReference<ConsumerRecord<String, String>>();
            await().atMost(Duration.ofSeconds(20)).until(() -> {
                for (var registro : leitor.poll(Duration.ofMillis(500))) {
                    if (ceId.equals(texto(registro, "ce_id"))
                            && GRUPO_DO_ESTOQUE.equals(texto(registro, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP))) {
                        encontrado.set(registro);
                    }
                }
                return encontrado.get() != null;
            });
            return encontrado.get();
        }
    }

    private List<ConsumerRecord<String, String>> lerDlq() {
        try (var leitor = leitor()) {
            var lidos = new ArrayList<ConsumerRecord<String, String>>();
            var prazo = Instant.now().plusSeconds(3);
            while (Instant.now().isBefore(prazo)) {
                leitor.poll(Duration.ofMillis(500)).forEach(lidos::add);
            }
            return lidos;
        }
    }

    private Consumer<String, String> leitor() {
        Map<String, Object> propriedades =
                KafkaTestUtils.consumerProps(embeddedKafka, "leitor-" + UUID.randomUUID(), false);
        propriedades.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<String, String> leitor = new DefaultKafkaConsumerFactory<>(propriedades,
                new StringDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(leitor, DLQ_RESERVAS);
        return leitor;
    }

    private List<String> origensDasRetiradas(String evento) {
        return eventoDoEstoqueRepository.lerStream(StreamDoEstoqueVO.de(evento, "PISTA")).stream()
                .map(EventoGravadoVO::getEvento)
                .filter(IngressoRetiradoEvent.class::isInstance)
                .map(IngressoRetiradoEvent.class::cast)
                .map(IngressoRetiradoEvent::getOrigemEventoId)
                .toList();
    }

    private int totalAgregado(String evento) {
        return agregacaoDeReservasService.listar(evento).stream()
                .mapToInt(AgregacaoDeSetorVO::getTotalIngressos)
                .sum();
    }

    private static String texto(ConsumerRecord<?, ?> registro, String nome) {
        var cabecalho = registro.headers().lastHeader(nome);
        return cabecalho == null ? null : new String(cabecalho.value(), StandardCharsets.UTF_8);
    }

    private static byte[] bytes(String valor) {
        return valor.getBytes(StandardCharsets.UTF_8);
    }
}
