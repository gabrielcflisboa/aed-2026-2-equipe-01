package br.pucminas.aed.ingressos.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import br.pucminas.aed.ingressos.domain.DeduplicacaoRepository;

@SpringBootTest
@Transactional
class RetencaoDaDeduplicacaoServiceTest {

    @Autowired
    private RetencaoDaDeduplicacaoService retencaoDaDeduplicacaoService;

    @Autowired
    private DeduplicacaoRepository deduplicacaoRepository;

    @Autowired
    private JdbcTemplate clienteJdbc;

    @Test
    @DisplayName("esquece o que passou da retencao e lembra o que ainda esta dentro dela")
    void esqueceSoOQuePassouDaRetencao() {
        var antigo = UUID.randomUUID();
        var recente = UUID.randomUUID();
        registrar("evento_processado", "processado_em", antigo, 50);
        registrar("evento_processado", "processado_em", recente, 1);
        registrar("evento_agregado", "agregado_em", antigo, 50);
        registrar("evento_agregado", "agregado_em", recente, 1);

        retencaoDaDeduplicacaoService.esquecerEntregasAntigas();

        assertThat(deduplicacaoRepository.jaProcessado(antigo)).isFalse();
        assertThat(deduplicacaoRepository.jaProcessado(recente)).isTrue();
        assertThat(agregados(antigo)).isZero();
        assertThat(agregados(recente)).isEqualTo(1);
    }

    private void registrar(String tabela, String coluna, UUID eventoId, int diasAtras) {
        clienteJdbc.update("INSERT INTO " + tabela + " (evento_id, " + coluna + ") VALUES (?, ?)",
                eventoId.toString(),
                OffsetDateTime.ofInstant(Instant.now().minus(diasAtras, ChronoUnit.DAYS), ZoneOffset.UTC));
    }

    private int agregados(UUID eventoId) {
        return clienteJdbc.queryForObject("SELECT COUNT(1) FROM evento_agregado WHERE evento_id = ?",
                Integer.class, eventoId.toString());
    }
}
