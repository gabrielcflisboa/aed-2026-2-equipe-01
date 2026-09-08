# Entrega da aula 05: event sourcing no estoque de ingressos

## O que foi feito nesta etapa

O `servico-ingressos` deixou de guardar o estoque numa tabela mutável e passou a
derivá-lo de um log append-only. A entrega vai até o event store e para ali: o
log, o agregado que se reconstrói a partir dele e a versão que detecta
concorrência. Nenhuma projeção entra aqui, e o motivo está na seção sobre
defasagem.

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
- **A decisão sai do log.** O `IngressoService` relê o stream, reconstrói o
  agregado e decide sobre ele. Não existe tabela de saldo para consultar, e
  quando existir uma, não é aqui que ela vai ser lida.
- **A capacidade virou fato.** Nada de `data.sql`: o `AberturaDeSetoresService`
  grava um `SetorAbertoEvent` no primeiro arranque, e por isso a capacidade
  sobrevive a qualquer releitura do log.
- **Idempotência preservada.** A dedup da aula 02 continua, agora com a chave
  primária fazendo o trabalho: tenta inserir e trata a colisão, em vez de
  perguntar antes e inserir depois.

### Correções de base que esta etapa exigiu

O `develop` não compilava nem subia antes desta entrega, e sem isso nada aqui
seria verificável. Foram corrigidos:

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

## A defasagem tolerada

Existe um leitor do estoque neste código, e ele é o próprio serviço decidindo
sobre uma reserva.

| Leitura | Quem faz | Lê de | Defasagem tolerada | Por que essa tolerância |
|---|---|---|---|---|
| **Aceitar ou recusar a reserva** | o próprio serviço, a cada mensagem | **o event store** | **zero** | É a única leitura que cria um fato. O agregado é reconstruído do stream toda vez, e a versão detecta quem gravou no meio do caminho. Atraso aqui não é inconveniente visual, é o mesmo assento vendido duas vezes. |

A tabela por tela vem junto com a projeção, e a ausência dela nesta entrega é
deliberada. Enquanto o log for o único lugar de onde alguém lê, não há defasagem
para tolerar, e um número declarado agora seria inventado. Quando a primeira
projeção entrar, cada tela que ela servir aparece aqui com a tolerância e a
justificativa, e a linha acima continua valendo: é ela que permite que as outras
sejam generosas.

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
```

Subir de novo não duplica nada: o `AberturaDeSetoresService` só grava em stream
vazio.

### Conferir o log

```bash
cd servico-ingressos && ./mvnw.cmd test
```

O que cada teste prova:

- `EventoDoEstoqueRepositoryTest`, o event store. Os eventos saem na ordem em que
  entraram, a versão é por stream e não global, e duas gravações feitas sobre a
  mesma leitura colidem, com a segunda virando `ConcorrenciaNoStreamException`.
  Confere também que o payload sobrevive à ida e volta do JSON e que `lerDesde`
  devolve o log em ordem global, que é a ordem de qualquer releitura.
- `EstoqueDoSetorTest`, o replay puro, sem banco: o estado é função do log e de
  mais nada.
- `IngressoServiceIdempotenciaTest`, a mesma mensagem entregue três vezes deixando
  um único `IngressoRetirado` no log.

Manual, com a aplicação parada e o `data/ingressos.mv.db` no lugar:

```sql
SELECT sequencia, stream_id, versao, tipo, dados
  FROM evento_do_estoque
 ORDER BY sequencia;
```

O que estiver ali continua ali depois de qualquer reserva seguinte. Nenhuma linha
é atualizada e nenhuma é removida, e é a `versao` crescendo de um em um dentro de
cada `stream_id` que mostra isso.

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

**O event store**, em `.../ingressos/service/`

- [EventoDoEstoqueJdbcRepository.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/EventoDoEstoqueJdbcRepository.java), o `INSERT` e a colisão de versão
- [IngressoService.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/IngressoService.java), dedup, replay, decisão, append
- [AberturaDeSetoresService.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/AberturaDeSetoresService.java), a capacidade entrando como fato
- [schema.sql](../../servico-ingressos/src/main/resources/schema.sql), o que é log e o que é memória de entrega

**Testes**

- [EventoDoEstoqueRepositoryTest.java](../../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/service/EventoDoEstoqueRepositoryTest.java), ordem, versão e concorrência
- [EstoqueDoSetorTest.java](../../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/domain/EstoqueDoSetorTest.java), o replay puro, sem banco
- [IngressoServiceIdempotenciaTest.java](../../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/service/IngressoServiceIdempotenciaTest.java), mesmo evento 3x, efeito único

**Registro de IA**: [docs/IA.md](../IA.md)

---

## Por onde começar a leitura

1. **ADR-005**, para a escolha do agregado e o que ela custou.
2. **`EstoqueDoSetor`**, para ver que o estado é função do log e mais nada. O
   agregado não tem construtor público.
3. **`EventoDoEstoqueJdbcRepository`**, para o `INSERT` sozinho e para a
   `DuplicateKeyException` virando `ConcorrenciaNoStreamException`.
4. **`IngressoService.processarReserva`**, para a sequência que importa: dedup,
   reconstrói pelo stream, decide, anexa.
5. **`EventoDoEstoqueRepositoryTest`**, para as garantias do log rodando.

---

## Estado da verificação

```
servico-vendas     mvn -o test    Tests run: 5,  Failures: 0, Errors: 0   BUILD SUCCESS
servico-ingressos  mvn -o test    Tests run: 15, Failures: 0, Errors: 0   BUILD SUCCESS
```

---

## Quem fez o quê

| Integrante | O que fez |
|---|---|
| Pedro Assis Corrêa | ADR-005; event store (`evento_do_estoque`, versão como detector de concorrência); agregado `EstoqueDoSetor` e os quatro eventos; a suíte de testes de replay, idempotência e concorrência; correção dos bloqueadores de build herdados da aula 02; esta folha de entrega. |
| `<nome>` | `<a preencher pela equipe>` |
| `<nome>` | `<a preencher pela equipe>` |
| `<nome>` | `<a preencher pela equipe>` |
| `<nome>` | `<a preencher pela equipe>` |
| `<nome>` | `<a preencher pela equipe>` |
