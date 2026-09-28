package br.pucminas.aed.ingressos;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ExceptionClassifier;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import tools.jackson.core.JacksonException;

@Configuration
public class ResilienciaConfig {

    static final List<Class<? extends Exception>> FALHAS_PERMANENTES = List.of(
            JacksonException.class,
            IllegalArgumentException.class,
            NullPointerException.class);

    private static final int TAMANHO_MAXIMO_DA_MENSAGEM = 500;

    private final String sufixoDlq;
    private final String nomeDoServico;

    public ResilienciaConfig(@Value("${app.resiliencia.sufixo-dlq}") String sufixoDlq,
            @Value("${spring.application.name}") String nomeDoServico) {
        this.sufixoDlq = sufixoDlq;
        this.nomeDoServico = nomeDoServico;
    }

    @Bean
    public NewTopic dlqDasReservas(@Value("${app.topico-reservas}") String topico,
            @Value("${app.resiliencia.retencao-dlq-dias}") long retencaoEmDias) {
        return dlq(topico, retencaoEmDias);
    }

    @Bean
    public NewTopic dlqDasLiberacoes(@Value("${app.topico-liberacoes}") String topico,
            @Value("${app.resiliencia.retencao-dlq-dias}") long retencaoEmDias) {
        return dlq(topico, retencaoEmDias);
    }

    @Bean
    public DefaultErrorHandler tratadorDeFalhas(KafkaTemplate<String, String> clienteDoBroker,
            @Value("${app.resiliencia.retentativas}") int retentativas,
            @Value("${app.resiliencia.espera-inicial-ms}") long esperaInicialMs,
            @Value("${app.resiliencia.multiplicador}") double multiplicador,
            @Value("${app.resiliencia.espera-maxima-ms}") long esperaMaximaMs) {
        var recuperador = new DeadLetterPublishingRecoverer(clienteDoBroker,
                (registro, falha) -> new TopicPartition(registro.topic() + sufixoDlq, -1));
        recuperador.setHeadersFunction((registro, falha) -> motivo(falha));

        var espera = new ExponentialBackOffWithMaxRetries(retentativas);
        espera.setInitialInterval(esperaInicialMs);
        espera.setMultiplier(multiplicador);
        espera.setMaxInterval(esperaMaximaMs);

        var tratador = new DefaultErrorHandler(recuperador, espera);
        FALHAS_PERMANENTES.forEach(tratador::addNotRetryableExceptions);
        tratador.setCommitRecovered(true);
        return tratador;
    }

    static boolean permanente(Throwable falha) {
        for (Throwable atual = falha; atual != null; atual = atual.getCause() == atual ? null : atual.getCause()) {
            for (var tipo : FALHAS_PERMANENTES) {
                if (tipo.isInstance(atual)) {
                    return true;
                }
            }
            for (var tipo : ExceptionClassifier.defaultFatalExceptionsList()) {
                if (tipo.isInstance(atual)) {
                    return true;
                }
            }
        }
        return false;
    }

    private NewTopic dlq(String topicoOriginal, long retencaoEmDias) {
        return TopicBuilder.name(topicoOriginal + sufixoDlq)
                .partitions(1)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(retencaoEmDias).toMillis()))
                .build();
    }

    private Headers motivo(Exception falha) {
        Throwable causa = falha instanceof ListenerExecutionFailedException && falha.getCause() != null
                ? falha.getCause()
                : falha;
        var cabecalhos = new RecordHeaders();
        cabecalhos.add("dlq_excecao", bytes(causa.getClass().getName()));
        cabecalhos.add("dlq_mensagem", bytes(resumir(causa.getMessage())));
        cabecalhos.add("dlq_classificacao", bytes(permanente(falha) ? "permanente" : "transitoria-esgotada"));
        cabecalhos.add("dlq_falhou_em", bytes(Instant.now().toString()));
        cabecalhos.add("dlq_servico", bytes(nomeDoServico));
        return cabecalhos;
    }

    private static String resumir(String mensagem) {
        if (mensagem == null) {
            return "sem mensagem";
        }
        return mensagem.length() <= TAMANHO_MAXIMO_DA_MENSAGEM
                ? mensagem
                : mensagem.substring(0, TAMANHO_MAXIMO_DA_MENSAGEM);
    }

    private static byte[] bytes(String valor) {
        return valor.getBytes(StandardCharsets.UTF_8);
    }
}
