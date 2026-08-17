package br.pucminas.aed.ingressos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.Acknowledgment;

import br.pucminas.aed.ingressos.controller.IngressoListener;

@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false",
        "spring.datasource.url=jdbc:h2:mem:ingressos-idempotencia;DB_CLOSE_DELAY=-1"
})
class IngressoListenerIdempotenciaTest {

    @Autowired
    private IngressoListener ingressoListener;

    @Autowired
    private JdbcTemplate clienteJdbc;

    @Test
    void aplicaReservaECompensacaoUmaUnicaVezQuandoCadaEventoChegaTresVezes() throws Exception {
        clienteJdbc.update("INSERT INTO estoque_setor (setor, quantidade_disponivel) VALUES (?, ?)",
                "IDEMPOTENCIA", 10);

        var ackReserva = mock(Acknowledgment.class);
        String reserva = """
                {"eventoId":"%s","evento":"Show de Teste","itens":[
                  {"setor":"IDEMPOTENCIA","quantidade":2,"precoUnitario":180.00}
                ]}
                """.formatted(UUID.randomUUID());

        ingressoListener.receberReserva(reserva, ackReserva);
        ingressoListener.receberReserva(reserva, ackReserva);
        ingressoListener.receberReserva(reserva, ackReserva);

        assertEquals(8, quantidadeDoSetor("IDEMPOTENCIA"));
        assertEquals(1, eventosProcessados());
        verify(ackReserva, times(3)).acknowledge();

        var ackCompensacao = mock(Acknowledgment.class);
        String compensacao = """
                {"eventoId":"%s","itens":[
                  {"setor":"IDEMPOTENCIA","quantidade":2,"precoUnitario":180.00}
                ]}
                """.formatted(UUID.randomUUID());

        ingressoListener.receberCompensacao(compensacao, ackCompensacao);
        ingressoListener.receberCompensacao(compensacao, ackCompensacao);
        ingressoListener.receberCompensacao(compensacao, ackCompensacao);

        assertEquals(10, quantidadeDoSetor("IDEMPOTENCIA"));
        assertEquals(2, eventosProcessados());
        verify(ackCompensacao, times(3)).acknowledge();
    }

    private Integer quantidadeDoSetor(String setor) {
        return clienteJdbc.queryForObject(
                "SELECT quantidade_disponivel FROM estoque_setor WHERE setor = ?", Integer.class, setor);
    }

    private Integer eventosProcessados() {
        return clienteJdbc.queryForObject("SELECT COUNT(1) FROM evento_processado", Integer.class);
    }
}