package br.pucminas.aed.ingressos.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import br.pucminas.aed.ingressos.domain.IngressoDevolvidoEvent;
import br.pucminas.aed.ingressos.domain.IngressoLiberadoEvent;
import br.pucminas.aed.ingressos.domain.IngressoReservadoEvent;
import br.pucminas.aed.ingressos.domain.IngressoRetiradoEvent;
import br.pucminas.aed.ingressos.domain.ItemDoIngressoVO;
import br.pucminas.aed.ingressos.domain.SetorAbertoEvent;
import tools.jackson.databind.ObjectMapper;

@SpringBootTest
@Transactional
class ConsultaDoEstoqueTest {

    private static final String EVENTO = "SHOW-CONSULTA";

    @Autowired
    private AberturaDeSetoresService aberturaDeSetoresService;

    @Autowired
    private IngressoService ingressoService;

    @Autowired
    private ObjectMapper conversorJson;

    @Test
    @DisplayName("a consulta mostra a retirada e a devolucao da reserva liberada, com o setor de volta ao total")
    void consultaMostraOEfeitoDesfeito() {
        aberturaDeSetoresService.abrir(EVENTO, "PISTA", 10);
        var reservaId = UUID.randomUUID();
        ingressoService.processarReserva(new IngressoReservadoEvent(reservaId, EVENTO,
                List.of(new ItemDoIngressoVO("PISTA", 2)), Instant.now()));
        ingressoService.processarLiberacao(new IngressoLiberadoEvent(UUID.randomUUID(), reservaId,
                "compra-consulta", EVENTO, List.of(new ItemDoIngressoVO("PISTA", 2)),
                "pagamento recusado", Instant.now()));

        var situacao = ingressoService.consultar("show-consulta", "pista");

        assertThat(situacao.getEvento()).isEqualTo(EVENTO);
        assertThat(situacao.getSetor()).isEqualTo("PISTA");
        assertThat(situacao.getRetirados()).isZero();
        assertThat(situacao.getDisponivel()).isEqualTo(10);
        assertThat(situacao.getHistorico()).extracting(fato -> fato.getTipo())
                .containsExactly(SetorAbertoEvent.TIPO, IngressoRetiradoEvent.TIPO, IngressoDevolvidoEvent.TIPO);

        var devolucao = (IngressoDevolvidoEvent) situacao.getHistorico().get(2).getDados();
        assertThat(devolucao.getReservaEventoId()).isEqualTo(reservaId.toString());
    }

    @Test
    @DisplayName("o JSON da consulta traz os campos de cada fato e as datas em ISO-8601")
    void jsonDaConsultaTrazOsCamposDosFatos() {
        aberturaDeSetoresService.abrir(EVENTO, "CAMAROTE", 4);
        var reservaId = UUID.randomUUID();
        ingressoService.processarReserva(new IngressoReservadoEvent(reservaId, EVENTO,
                List.of(new ItemDoIngressoVO("CAMAROTE", 1)), Instant.now()));

        var json = conversorJson.writeValueAsString(ingressoService.consultar(EVENTO, "CAMAROTE"));

        assertThat(json)
                .contains("\"tipo\":\"IngressoRetirado\"")
                .contains("\"origemEventoId\":\"" + reservaId + "\"")
                .containsPattern("\"gravadoEm\":\"\\d{4}-\\d{2}-\\d{2}T");
    }
}
