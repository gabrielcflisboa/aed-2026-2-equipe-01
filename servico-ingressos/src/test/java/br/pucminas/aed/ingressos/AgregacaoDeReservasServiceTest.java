package br.pucminas.aed.ingressos;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import br.pucminas.aed.ingressos.domain.AgregacaoDeSetorVO;
import br.pucminas.aed.ingressos.domain.IngressoReservadoEvent;
import br.pucminas.aed.ingressos.domain.ItemDoIngressoVO;
import br.pucminas.aed.ingressos.service.AgregacaoDeReservasService;

@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false",
        "spring.datasource.url=jdbc:h2:mem:ingressos-agregacao;DB_CLOSE_DELAY=-1"
})
class AgregacaoDeReservasServiceTest {

    @Autowired
    private AgregacaoDeReservasService agregacaoDeReservasService;

    @Autowired
    private JdbcTemplate clienteJdbc;

    @Test
    void somaDuasReservasNaMesmaJanelaDeUmMinuto() {
        var evento = "show-agregacao-1";
        agregacaoDeReservasService.agregar(reserva(evento, "PISTA", 2, "2026-01-01T10:00:10Z"));
        agregacaoDeReservasService.agregar(reserva(evento, "PISTA", 3, "2026-01-01T10:00:45Z"));

        var janelas = agregacaoDeReservasService.listar(evento);

        assertEquals(1, janelas.size());
        assertEquals(Instant.parse("2026-01-01T10:00:00Z"), janelas.get(0).getJanelaInicio());
        assertEquals(5, janelas.get(0).getTotalIngressos());
    }

    @Test
    void reservasEmMinutosDiferentesGeramJanelasSeparadas() {
        var evento = "show-agregacao-2";
        agregacaoDeReservasService.agregar(reserva(evento, "CAMAROTE", 1, "2026-01-01T11:00:05Z"));
        agregacaoDeReservasService.agregar(reserva(evento, "CAMAROTE", 4, "2026-01-01T11:01:05Z"));

        var janelas = agregacaoDeReservasService.listar(evento);

        assertEquals(2, janelas.size());
        assertEquals(1, totalDaJanela(janelas, "2026-01-01T11:00:00Z"));
        assertEquals(4, totalDaJanela(janelas, "2026-01-01T11:01:00Z"));
    }

    // Sem watermark: um evento atrasado (processado por ultimo, mas com reservadoEm de uma
    // janela anterior) ainda atualiza a janela correta, mesmo que uma janela mais nova ja
    // tenha sido processada antes dele.
    @Test
    void eventoAtrasadoAindaAtualizaAJanelaDeOcorrenciaCorreta() {
        var evento = "show-agregacao-3";
        agregacaoDeReservasService.agregar(reserva(evento, "ARQUIBANCADA", 2, "2026-01-01T12:05:00Z"));
        agregacaoDeReservasService.agregar(reserva(evento, "ARQUIBANCADA", 6, "2026-01-01T12:00:30Z"));

        var janelas = agregacaoDeReservasService.listar(evento);

        assertEquals(2, janelas.size());
        assertEquals(6, totalDaJanela(janelas, "2026-01-01T12:00:00Z"));
        assertEquals(2, totalDaJanela(janelas, "2026-01-01T12:05:00Z"));
    }

    // Pergunta 4 da aula-03.md: reprocessar o fluxo do comeco, com a tabela de agregacao
    // zerada, produz os mesmos totais. Aqui o segundo processamento ainda chega em ordem
    // diferente da primeira, porque a janela depende so de reservadoEm e nao da chegada.
    @Test
    void reprocessarDoComecoComATabelaZeradaDaOsMesmosTotais() {
        var evento = "show-agregacao-4";
        var reservas = List.of(
                reserva(evento, "PISTA", 2, "2026-01-01T13:00:10Z"),
                reserva(evento, "PISTA", 3, "2026-01-01T13:00:50Z"),
                reserva(evento, "PISTA", 4, "2026-01-01T13:01:05Z"),
                reserva(evento, "CAMAROTE", 1, "2026-01-01T13:01:30Z"));

        reservas.forEach(agregacaoDeReservasService::agregar);
        var primeiraLeitura = fotografia(evento);

        clienteJdbc.update("DELETE FROM agregacao_reserva_por_setor_janela WHERE evento = ?", evento);
        assertEquals(0, agregacaoDeReservasService.listar(evento).size());

        reservas.reversed().forEach(agregacaoDeReservasService::agregar);
        var segundaLeitura = fotografia(evento);

        assertEquals(3, primeiraLeitura.size());
        assertEquals(primeiraLeitura, segundaLeitura);
    }

    // Uma linha por (setor, janela, total), ordenada, para comparar sem depender da ordem
    // em que o banco devolve as linhas.
    private List<String> fotografia(String evento) {
        return agregacaoDeReservasService.listar(evento).stream()
                .map(janela -> "%s|%s|%d".formatted(
                        janela.getSetor(), janela.getJanelaInicio(), janela.getTotalIngressos()))
                .sorted()
                .toList();
    }

    private int totalDaJanela(List<AgregacaoDeSetorVO> janelas, String janelaInicioIso) {
        var janelaInicio = Instant.parse(janelaInicioIso);
        return janelas.stream()
                .filter(janela -> janela.getJanelaInicio().equals(janelaInicio))
                .findFirst()
                .orElseThrow()
                .getTotalIngressos();
    }

    private IngressoReservadoEvent reserva(String evento, String setor, int quantidade, String reservadoEm) {
        return new IngressoReservadoEvent(
                UUID.randomUUID(),
                evento,
                List.of(new ItemDoIngressoVO(setor, quantidade)),
                Instant.parse(reservadoEm));
    }
}
