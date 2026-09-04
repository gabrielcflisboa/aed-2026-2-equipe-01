package br.pucminas.aed.ingressos.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import br.pucminas.aed.ingressos.domain.ConcorrenciaNoStreamException;
import br.pucminas.aed.ingressos.domain.EstoqueDoSetor;
import br.pucminas.aed.ingressos.domain.EstoqueEvent;
import br.pucminas.aed.ingressos.domain.EventoDoEstoqueRepository;
import br.pucminas.aed.ingressos.domain.IngressoRetiradoEvent;
import br.pucminas.aed.ingressos.domain.SetorAbertoEvent;
import br.pucminas.aed.ingressos.domain.StreamDoEstoqueVO;

@SpringBootTest
@Transactional
class EventoDoEstoqueRepositoryTest {

    @Autowired
    private EventoDoEstoqueRepository eventoDoEstoqueRepository;

    @Autowired
    private JdbcTemplate clienteJdbc;

    @Test
    @DisplayName("os eventos saem do stream na ordem em que entraram, com versao crescente")
    void ordemEVersaoPorStream() {
        var stream = streamNovo();

        eventoDoEstoqueRepository.anexar(stream, EstoqueDoSetor.VERSAO_DE_STREAM_VAZIO,
                List.of(new SetorAbertoEvent(10)));
        eventoDoEstoqueRepository.anexar(stream, 1L,
                List.of(new IngressoRetiradoEvent(2, "msg-1"), new IngressoRetiradoEvent(3, "msg-2")));

        var log = eventoDoEstoqueRepository.lerStream(stream);

        assertThat(log).extracting(g -> g.getVersao()).containsExactly(1L, 2L, 3L);
        assertThat(log).extracting(g -> g.getEvento().tipo())
                .containsExactly(SetorAbertoEvent.TIPO, IngressoRetiradoEvent.TIPO, IngressoRetiradoEvent.TIPO);
        assertThat(log).extracting(g -> g.getSequencia()).isSorted();
    }

    @Test
    @DisplayName("a versao detecta concorrencia: duas gravacoes sobre a mesma leitura, uma passa")
    void versaoDetectaConcorrencia() {
        var stream = streamNovo();
        eventoDoEstoqueRepository.anexar(stream, EstoqueDoSetor.VERSAO_DE_STREAM_VAZIO,
                List.of(new SetorAbertoEvent(10)));

        var versaoLidaPelosDois = 1L;

        eventoDoEstoqueRepository.anexar(stream, versaoLidaPelosDois,
                List.of(new IngressoRetiradoEvent(4, "msg-a")));

        assertThatThrownBy(() -> eventoDoEstoqueRepository.anexar(stream, versaoLidaPelosDois,
                List.of(new IngressoRetiradoEvent(4, "msg-b"))))
                .isInstanceOf(ConcorrenciaNoStreamException.class)
                .hasMessageContaining("alem da versao 1");
    }

    @Test
    @DisplayName("cada stream tem a propria numeracao de versao")
    void versaoEPorStreamNaoGlobal() {
        var pista = streamNovo();
        var camarote = StreamDoEstoqueVO.de(pista.getEvento(), "CAMAROTE");

        eventoDoEstoqueRepository.anexar(pista, 0L, List.of(new SetorAbertoEvent(10)));
        eventoDoEstoqueRepository.anexar(camarote, 0L, List.of(new SetorAbertoEvent(4)));

        assertThat(eventoDoEstoqueRepository.lerStream(pista)).singleElement()
                .extracting(g -> g.getVersao()).isEqualTo(1L);
        assertThat(eventoDoEstoqueRepository.lerStream(camarote)).singleElement()
                .extracting(g -> g.getVersao()).isEqualTo(1L);
    }

    @Test
    @DisplayName("lerDesde devolve o log em ordem global, que e a ordem da releitura")
    void ordemGlobalParaAReleitura() {
        var pista = streamNovo();
        var camarote = StreamDoEstoqueVO.de(pista.getEvento(), "CAMAROTE");
        var antes = ultimaSequencia();

        eventoDoEstoqueRepository.anexar(pista, 0L, List.of(new SetorAbertoEvent(10)));
        eventoDoEstoqueRepository.anexar(camarote, 0L, List.of(new SetorAbertoEvent(4)));
        eventoDoEstoqueRepository.anexar(pista, 1L, List.of(new IngressoRetiradoEvent(1, "msg-1")));

        var log = eventoDoEstoqueRepository.lerDesde(antes, 100);

        assertThat(log).hasSize(3);
        assertThat(log).extracting(g -> g.getStream().getSetor())
                .containsExactly("PISTA", "CAMAROTE", "PISTA");
        assertThat(log).extracting(g -> g.getSequencia()).isSorted();
    }

    @Test
    @DisplayName("o payload sobrevive a ida e volta do JSON")
    void payloadPreservado() {
        var stream = streamNovo();
        EstoqueEvent original = new IngressoRetiradoEvent(7, "msg-42");

        eventoDoEstoqueRepository.anexar(stream, 0L, List.of(original));

        assertThat(eventoDoEstoqueRepository.lerStream(stream))
                .singleElement()
                .extracting(g -> g.getEvento())
                .isEqualTo(original);
    }

    private StreamDoEstoqueVO streamNovo() {
        return StreamDoEstoqueVO.de("show-" + UUID.randomUUID(), "PISTA");
    }

    private long ultimaSequencia() {
        var sequencia = clienteJdbc.queryForObject(
                "SELECT COALESCE(MAX(sequencia), 0) FROM evento_do_estoque", Long.class);
        return sequencia == null ? 0L : sequencia;
    }
}
