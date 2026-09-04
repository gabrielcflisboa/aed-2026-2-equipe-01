# Entrega da aula 05: event sourcing e projeções

## O que foi feito nesta etapa

O `servico-ingressos` deixou de guardar o estoque numa tabela mutável e passou a
derivá-lo de um log append-only.

- **Agregado e ADR.** `EstoqueDoSetor`, com um stream por `(evento, setor)`. A
  escolha, as alternativas descartadas e as consequências aceitas estão em
  [ADR-005](../adr/ADR-005-event-sourcing.md).
- **Event store.** Tabela `evento_do_estoque`, append-only: o único comando de
  escrita em todo o código é um `INSERT`. Ordem global por `sequencia`, ordem por
  stream em `versao`, e `UNIQUE (stream_id, versao)` como detector de
  concorrência.
- **Quatro fatos.** `SetorAberto`, `IngressoRetirado`, `IngressoDevolvido`, que é
  a compensação prometida no ADR-002, e `ReservaRecusada`, porque a recusa também
  é informação: é ela que responde quantos tentaram comprar depois de esgotar.
- **Duas projeções**, derivadas e descartáveis: `disponibilidade_por_setor`, a
  tela do comprador, e `ocupacao_por_evento`, a tela da produção do show. As duas
  avançam por catch-up periódico a partir de um checkpoint próprio.
- **A decisão não lê projeção.** O `IngressoService` relê o stream, reconstrói o
  agregado e decide sobre ele. Nenhuma regra de negócio consulta uma tabela de
  projeção.
- **A capacidade virou fato.** Nada de `data.sql`: o `AberturaDeSetoresService`
  grava um `SetorAbertoEvent` no primeiro arranque, e por isso a capacidade
  sobrevive a qualquer reconstrução.
- **Idempotência preservada.** A dedup da aula 02 continua, agora com a chave
  primária fazendo o trabalho: tenta inserir e trata a colisão, em vez de
  perguntar antes e inserir depois.

### Correções de base que esta etapa exigiu

O `develop` não compilava nem subia antes desta entrega, e sem isso o critério
"apague a projeção e reconstrua" não seria verificável. Foram corrigidos:

- O `VendaService` chamava um método inexistente no `VendaCallbackService`
  (`registrar`). A publicação agora é delegada ao `publicar`, com um envelope
  CloudEvents só, o que também eliminou o `ce_time = Instant.now()` que o
  [docs/IA.md](../IA.md) registra como decisão recusada.
- O `servico-ingressos` não tinha configuração de Kafka. O `topics` apontava para
  uma propriedade inexistente, e faltavam `ack-mode: manual`, deserializer e
  `spring.datasource.url`. O H2 subia em memória apesar de quatro arquivos
  prometerem H2 em arquivo.
- A cota por CPF passou a normalizar o documento. Antes, `000.000.000-00` e
  `00000000000` contavam como compradores diferentes.

---

## A defasagem tolerada, por tela

Projeção é assíncrona, então existe uma janela em que o log já tem o fato e a
tela ainda não. O intervalo de catch-up é `app.projecoes.intervalo`, hoje em
**1000 ms**. Esse é o teto da defasagem de qualquer tela, e é menor do que a
tolerância de todas elas.

| Tela | Quem olha | Lê de | Defasagem tolerada | Por que essa tolerância |
|---|---|---|---|---|
| **Disponibilidade no setor** | comprador, na hora de escolher | `disponibilidade_por_setor` | **até ~2 s** | Um número atrasado aqui só faz o comprador tentar uma reserva que o agregado vai recusar. Ele não permite overselling, porque quem decide não lê esta tabela. O custo do atraso é uma tentativa frustrada, não um ingresso vendido duas vezes, e por isso a tolerância pode ser folgada. |
| **Ocupação do evento** | produção do show, acompanhando a venda | `ocupacao_por_evento` | **até 5 min** | É insumo de decisão de marketing e logística, como abrir mais um lote ou reforçar divulgação, tomada em escala de horas. Ninguém aperta um botão por segundo olhando esta tela, e um número de cinco minutos atrás leva à mesma decisão que o número de agora. |
| **Demanda recusada** | produção do show, no mesmo painel | `ocupacao_por_evento` (`reservas_recusadas`) | **até 1 h** | Serve para dimensionar o próximo lote ou a próxima data. É leitura de tendência, e a diferença entre 40 e 43 recusas não muda decisão nenhuma. |
| **Aceitar ou recusar a reserva** | o próprio serviço | **o event store** | **zero, não lê projeção** | É a única leitura que cria um fato. O agregado é reconstruído do stream a cada mensagem, e a versão detecta quem gravou no meio do caminho. Se esta decisão lesse a projeção para ganhar velocidade, a projeção viraria fonte da verdade, e o atraso que é inofensivo nas linhas de cima passaria a vender o mesmo assento duas vezes. |

A linha de baixo é a que justifica todas as outras. A tolerância das telas pode
ser generosa exatamente porque nenhuma decisão depende delas.

---

## Como rodar

Pré-requisitos: JDK 21 e Docker. O Maven vem pelo wrapper.

```powershell
docker compose up -d
cd servico-ingressos; ./mvnw.cmd spring-boot:run
```

Em outro terminal:

```powershell
cd servico-vendas; ./mvnw.cmd spring-boot:run
curl -X POST http://localhost:8080/vendas/reservas -H "Content-Type: application/json" -d '@../exemplo-reserva.json'
```

No arranque, o `servico-ingressos` abre os setores configurados em
`app.abertura`, gravando `SetorAbertoEvent` em vez de linhas de estoque:

```
setor aberto: SHOW-PUCMINAS-2026::PISTA com capacidade 100
setor aberto: SHOW-PUCMINAS-2026::CAMAROTE com capacidade 20
setor aberto: SHOW-PUCMINAS-2026::ARQUIBANCADA com capacidade 50
projecao disponibilidade_por_setor avancou 3 evento(s)
projecao ocupacao_por_evento avancou 3 evento(s)
```

### Apagar a projeção e reconstruir pelo log

É o critério que mais pesa na correção, e há dois caminhos.

Automatizado, que é o que prova de verdade:

```bash
cd servico-ingressos && ./mvnw.cmd test -Dtest=ReconstrucaoDeProjecaoTest
```

O teste monta um histórico com os quatro tipos de fato, guarda o conteúdo das
duas projeções, apaga as duas tabelas inteiras, reconstrói pelo log e compara
linha a linha. Também confere que o log não foi tocado no caminho, comparando
`contar()` antes e depois.

Manual, com a aplicação parada e o `data/ingressos.mv.db` no lugar:

```sql
DELETE FROM disponibilidade_por_setor;
DELETE FROM ocupacao_por_evento;
DELETE FROM projecao_checkpoint;
```

Suba a aplicação de novo. Não há comando especial de reconstrução: como o
checkpoint voltou a zero, o mesmo catch-up de sempre relê o log do início e chega
ao mesmo estado. Os números da `disponibilidade_por_setor` voltam idênticos. O
teste `apagarTabelasECheckpointReconstroiPeloCatchUp` cobre exatamente este
procedimento.

Para abrir o banco:

```bash
java -cp ~/.m2/repository/com/h2database/h2/2.4.240/h2-2.4.240.jar org.h2.tools.Shell -url "jdbc:h2:file:./servico-ingressos/data/ingressos;AUTO_SERVER=TRUE" -user sa -password ""
```

---

## Onde está cada coisa

**Decisão**

- [ADR-005, event sourcing no estoque](../adr/ADR-005-event-sourcing.md)
- [ADR-002, domínio do projeto](../adr/ADR-002-dominio-do-projeto.md)
- [Contrato do evento de mensageria](../contrato.md)

**O agregado e os fatos**, em `servico-ingressos/src/main/java/br/pucminas/aed/ingressos/domain/`

- [EstoqueDoSetor.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/domain/EstoqueDoSetor.java), replay e decisão
- [StreamDoEstoqueVO.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/domain/StreamDoEstoqueVO.java), a escolha do agregado em código
- [EstoqueEvent.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/domain/EstoqueEvent.java) e os quatro fatos
- [EventoDoEstoqueRepository.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/domain/EventoDoEstoqueRepository.java), o contrato sem `UPDATE` e sem `DELETE`

**O event store e as projeções**, em `.../ingressos/service/`

- [EventoDoEstoqueJdbcRepository.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/EventoDoEstoqueJdbcRepository.java)
- [DisponibilidadeProjecaoService.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/DisponibilidadeProjecaoService.java)
- [OcupacaoProjecaoService.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/OcupacaoProjecaoService.java)
- [ReconstrucaoService.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/ReconstrucaoService.java), catch-up e replay
- [IngressoService.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/IngressoService.java), dedup, replay, decisão, append
- [schema.sql](../../servico-ingressos/src/main/resources/schema.sql), o que é log, o que é memória de entrega, o que é projeção

**Testes**

- [ReconstrucaoDeProjecaoTest.java](../../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/service/ReconstrucaoDeProjecaoTest.java), o critério que mais pesa
- [IngressoServiceIdempotenciaTest.java](../../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/service/IngressoServiceIdempotenciaTest.java), mesmo evento 3x, efeito único
- [EventoDoEstoqueRepositoryTest.java](../../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/service/EventoDoEstoqueRepositoryTest.java), ordem, versão e concorrência
- [EstoqueDoSetorTest.java](../../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/domain/EstoqueDoSetorTest.java), o replay puro, sem banco

**Registro de IA**: [docs/IA.md](../IA.md)

---

## Por onde começar a leitura

1. **ADR-005**, para a escolha do agregado e o que ela custou.
2. **`EstoqueDoSetor`**, para ver que o estado é função do log e mais nada. O
   agregado não tem construtor público.
3. **`IngressoService.processarReserva`**, para a sequência que importa: dedup,
   reconstrói pelo stream, decide, anexa. Repare que não há leitura de projeção.
4. **`ReconstrucaoService`**, para a diferença entre `avancar`, o caminho normal
   que produz a defasagem, e `reconstruir`, que joga fora e refaz.
5. **`ReconstrucaoDeProjecaoTest`**, para o critério da aula rodando.

---

## Estado da verificação

```
servico-vendas     mvn -o test    Tests run: 5,  Failures: 0, Errors: 0   BUILD SUCCESS
servico-ingressos  mvn -o test    Tests run: 20, Failures: 0, Errors: 0   BUILD SUCCESS
```

---

## Quem fez o quê

| Integrante | O que fez |
|---|---|
| Pedro Assis Corrêa | ADR-005; event store (`evento_do_estoque`, versão como detector de concorrência); agregado `EstoqueDoSetor` e os quatro eventos; as duas projeções e o `ReconstrucaoService`; a suíte de testes de replay, idempotência e concorrência; correção dos bloqueadores de build herdados da aula 02; esta folha de entrega. |
| `<nome>` | `<a preencher pela equipe>` |
| `<nome>` | `<a preencher pela equipe>` |
| `<nome>` | `<a preencher pela equipe>` |
| `<nome>` | `<a preencher pela equipe>` |
| `<nome>` | `<a preencher pela equipe>` |
