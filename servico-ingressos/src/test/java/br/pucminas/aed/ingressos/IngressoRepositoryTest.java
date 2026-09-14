package br.pucminas.aed.ingressos;

import br.pucminas.aed.ingressos.domain.IngressoRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional
class IngressoRepositoryTest {

    @Autowired
    private IngressoRepository repository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("Deve verificar se as tabelas existem, inserir e debitar estoque via JdbcTemplate")
    void testarFluxoJdbc() {
        // 1. Inserir estoque inicial de teste diretamente para validar que a tabela existe
        jdbcTemplate.update("INSERT INTO estoque_setor (setor, quantidade_disponivel) VALUES (?, ?)", "TEST_PISTA", 10);

        // 2. Testar o débito de estoque (UPDATE)
        boolean debitou = repository.debitarEstoque("TEST_PISTA", 2);
        assertTrue(debitou, "Deveria ter debitado 2 ingressos da TEST_PISTA");

        // 3. Confirmar que o valor no banco atualizou para 8
        Integer quantidadeRestante = jdbcTemplate.queryForObject(
                "SELECT quantidade_disponivel FROM estoque_setor WHERE setor = ?", Integer.class, "TEST_PISTA");
        assertEquals(8, quantidadeRestante);

        // 4. Testar o registro de evento processado (INSERT)
        UUID eventoId = UUID.randomUUID();
        assertFalse(repository.existeEvento(eventoId));

        repository.registrarEvento(eventoId);
        assertTrue(repository.existeEvento(eventoId), "Evento deveria constar como processado");
    }
}