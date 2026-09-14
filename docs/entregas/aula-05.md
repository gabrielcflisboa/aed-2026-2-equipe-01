# Entrega da aula 05: event sourcing e projeção no estoque de ingressos

## O que foi feito nesta etapa

O `servico-ingressos` deixou de guardar o estoque numa tabela mutável e passou a derivá-lo de um log append-only no
event store, além de disponibilizar a **projeção de disponibilidade por setor** para consultas otimizadas de leitura.

- **Agregado, Projeção e ADRs.** `EstoqueDoSetor`, com um stream por `(evento, setor)`. A escolha do agregado, as
  alternativas descartadas e as consequências aceitas estão em [ADR-005](../adr/ADR-005-event-sourcing.md). As decisões
  sobre a visão de leitura, atraso tolerado e replay determinístico estão
  em [ADR-006](../adr/ADR-006-projecoes-e-replay.md).
- **Event store.** Tabela `evento_do_estoque`, append-only: o único comando de escrita em todo o código é um `INSERT`.
  Ordem global por `sequencia`, ordem por stream em `versao`, e `UNIQUE (stream_id, versao)` como detector de
  concorrência.
- **Quatro fatos.** `SetorAberto`, `IngressoRetirado`, `IngressoDevolvido`, que é a compensação prometida no ADR-002, e
  `ReservaRecusada`, porque a recusa também é informação: é ela que responde quantos tentaram comprar depois de esgotar.
- **A decisão sai do log, a consulta sai da projeção.** O `IngressoService` relê o stream, reconstrói o agregado e
  decide sobre ele (consistência forte). A consulta rápida para vitrine é servida pela tabela read-only
  `disponibilidade_por_setor`.
- **Projeção incremental e checkpoint.** O `DisponibilidadeProjecaoService` consome os eventos em lotes e atualiza o
  estado de leitura. O progresso de leitura é mantido na tabela `projecao_checkpoint`, e o avanço é orquestrado de forma
  assíncrona pelo `ReconstrucaoService` via `@Scheduled` no intervalo configurado em `app.projecoes.intervalo`.
- **Replay determinístico.** A tabela `disponibilidade_por_setor` e seu checkpoint podem ser zerados e reconstruídos a
  qualquer momento pelo log a partir da sequência zero, garantindo idempotência e resiliência a edições externas
  indevidas.
- **A capacidade virou fato.** Nada de `data.sql`: o `AberturaDeSetoresService` grava um `SetorAbertoEvent` no primeiro
  arranque, e por isso a capacidade sobrevive a qualquer releitura do log.
- **Idempotência preservada.** A dedup da aula 02 continua, agora com a chave primária fazendo o trabalho: tenta inserir
  e trata a colisão, em vez de perguntar antes e inserir depois.

### Correções de base que esta etapa exigiu

O `develop` não compilava nem subia antes desta entrega, e sem isso nada aqui seria verificável. Foram corrigidos:

- O `VendaService` chamava um método inexistente no `VendaCallbackService` (`registrar`). A publicação agora é delegada
  ao `publicar`, com um envelope CloudEvents só, o que também eliminou o `ce_time = Instant.now()` que
  o [docs/IA.md](../IA.md) registra como decisão recusada.
- O `servico-ingressos` não tinha configuração de Kafka. O `topics` apontava para uma propriedade inexistente, e
  faltavam `ack-mode: manual`, deserializer e `spring.datasource.url`. O H2 subia em memória apesar de quatro arquivos
  prometerem H2 em arquivo.
- A cota por CPF passou a normalizar o documento. Antes, `000.000.000-00` e `00000000000` contavam como compradores
  diferentes.
- O consumidor Kafka do `servico-ingressos` nunca processava uma reserva de verdade: `IngressoListener` declarava o
  parâmetro do `@KafkaListener` já tipado (`IngressoReservadoEvent`), esperando que o `value-deserializer`
  (`JacksonJsonDeserializer`) do `application.yml` convertesse a mensagem antes de entregá-la. Em runtime real (broker
  de verdade, não o teste que chama o listener diretamente), a conversão não acontecia — a mensagem chegava como
  `String` e o listener derrubava toda reserva com `MessageConversionException`, sem nenhum evento além dos três
  `SetorAberto` no log. Corrigido lendo o payload como `String` e desserializando manualmente com `ObjectMapper`
  (`tools.jackson.databind`, a API Jackson 3 que o Spring Boot 4.1 usa), o mesmo padrão já validado no publisher. Só
  apareceu subindo os dois serviços com Kafka de verdade e enviando reservas reais — nenhum teste automatizado
  exercita esse caminho, porque todos chamam o listener em processo.

---

## A defasagem tolerada

| Leitura                                    | Quem faz                             | Lê de                                      | Defasagem tolerada                       | Por que essa tolerância                                                                                                                  |
|--------------------------------------------|--------------------------------------|--------------------------------------------|------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| **Aceitar ou recusar a reserva**           | O próprio serviço, a cada mensagem   | **Event Store** (`evento_do_estoque`)      | **Zero**                                 | É a leitura transacional que cria fatos. O agregado é reconstruído do stream toda vez e a versão detecta quem gravou no meio do caminho. |
| **Consulta de saldo/capacidade por setor** | Telas de vitrine, busca e dashboards | **Projeção** (`disponibilidade_por_setor`) | **Eventual** (`app.projecoes.intervalo`) | A leitura de interface tolera pequeno atraso de sincronização (ex: 1s) para não sobrecarregar o log de escrita.                          |

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

No arranque, o `servico-ingressos` abre os setores configurados em `app.abertura`, gravando `SetorAbertoEvent` em vez de
linhas de estoque:

```
setor aberto: SHOW-PUCMINAS-2026::PISTA com capacidade 100
setor aberto: SHOW-PUCMINAS-2026::CAMAROTE com capacidade 20
setor aberto: SHOW-PUCMINAS-2026::ARQUIBANCADA com capacidade 50

```

Subir de novo não duplica nada: o `AberturaDeSetoresService` só grava em stream vazio.

### Conferir o log e as projeções

```bash
cd servico-ingressos && ./mvnw.cmd test

```

O que cada teste prova:

* `EventoDoEstoqueRepositoryTest`, o event store. Os eventos saem na ordem em que entraram, a versão é por stream e não
  global, e duas gravações feitas sobre a mesma leitura colidem, com a segunda virando `ConcorrenciaNoStreamException`.
  Confere também que o payload sobrevive à ida e volta do JSON e que `lerDesde` devolve o log em ordem global, que é a
  ordem de qualquer releitura.
* `EstoqueDoSetorTest`, o replay puro, sem banco: o estado é função do log e de mais nada.
* `IngressoServiceIdempotenciaTest`, a mesma mensagem entregue três vezes deixando um único `IngressoRetirado` no log.
* `ReconstrucaoDeProjecaoTest`, a projeção e o replay. Prova que zerar a tabela de projeção e o checkpoint e reprocessar
  o log reconstrói exatamente o mesmo estado, que alterações externas indevidas são corrigidas na reconstrução e que
  novos fatos gravados são consumidos no ciclo agendado seguinte.

Manual, com a aplicação parada e o `data/ingressos.mv.db` no lugar:

```sql
SELECT sequencia, stream_id, versao, tipo, dados
FROM evento_do_estoque
ORDER BY sequencia;

```

E para conferir a tabela de projeção e checkpoint:

```sql
SELECT *
FROM disponibilidade_por_setor;
SELECT *
FROM projecao_checkpoint;

```

---

## Onde está cada coisa

**Decisão**

* [ADR-005, event sourcing no estoque](../adr/ADR-005-event-sourcing.md)
* [ADR-006, projeções e reconstrução de estado](../adr/ADR-006-projecoes-e-replay.md)
* [ADR-002, domínio do projeto](../adr/ADR-002-dominio-do-projeto.md)
* [Contrato do evento de mensageria](../contrato.md)

**O agregado e os fatos**, em `servico-ingressos/src/main/java/br/pucminas/aed/ingressos/domain/`

* [EstoqueDoSetor.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/domain/EstoqueDoSetor.java),
  replay e decisão
* [StreamDoEstoqueVO.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/domain/StreamDoEstoqueVO.java),
  a escolha do agregado em código
* [EstoqueEvent.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/domain/EstoqueEvent.java)
  e os quatro fatos
* [EventoDoEstoqueRepository.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/domain/EventoDoEstoqueRepository.java),
  o contrato sem `UPDATE` e sem `DELETE`

**O event store e a projeção**, em `.../ingressos/service/`

* [EventoDoEstoqueJdbcRepository.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/EventoDoEstoqueJdbcRepository.java),
  o `INSERT` e a colisão de versão
* [IngressoService.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/IngressoService.java),
  dedup, replay, decisão, append
* [DisponibilidadeProjecaoService.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/DisponibilidadeProjecaoService.java),
  a atualização da visão de leitura
* [ProjecaoCheckpointJdbcRepository.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/ProjecaoCheckpointJdbcRepository.java),
  persistência do ponteiro de leitura
* [ReconstrucaoService.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/ReconstrucaoService.java),
  orquestração de replay e tarefas `@Scheduled`
* [AberturaDeSetoresService.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/AberturaDeSetoresService.java),
  a capacidade entrando como fato
* [schema.sql](../../servico-ingressos/src/main/resources/schema.sql), o que é log, o
  que é projeção e o que é memória de entrega

**Testes**

* [EventoDoEstoqueRepositoryTest.java](../../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/service/EventoDoEstoqueRepositoryTest.java),
  ordem, versão e concorrência
* [EstoqueDoSetorTest.java](../../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/domain/EstoqueDoSetorTest.java),
  o replay puro, sem banco
* [IngressoServiceIdempotenciaTest.java](../../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/service/IngressoServiceIdempotenciaTest.java),
  mesmo evento 3x, efeito único
* [ReconstrucaoDeProjecaoTest.java](../../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/service/ReconstrucaoDeProjecaoTest.java),
  garantia de replay, reconstrução idempotente e agendamento

---

## Por onde começar a leitura

1. **ADR-005 e ADR-006**, para a escolha do agregado, projeções e o que elas custaram.
2. **`EstoqueDoSetor`**, para ver que o estado é função do log e mais nada.
3. **`DisponibilidadeProjecaoService` e `ReconstrucaoService**`, para entender como o estado de leitura é montado e
   atualizado assincronamente.
4. **`IngressoService.processarReserva`**, para a sequência que importa: dedup, reconstrói pelo stream, decide, anexa.
5. **`ReconstrucaoDeProjecaoTest`**, para ver as garantias de replay e reconstrução rodando na prática.

---

## Estado da verificação

```
servico-vendas     mvn -o test    Tests run: 5,  Failures: 0, Errors: 0   BUILD SUCCESS
servico-ingressos  mvn -o test    Tests run: 21, Failures: 0, Errors: 0   BUILD SUCCESS

```

Além da suíte automatizada, os dois serviços foram subidos de verdade (Kafka via `docker compose`) e exercitados por
HTTP: seis reservas de CAMAROTE (capacidade 20, quatro ingressos cada) resultaram em cinco `IngressoRetirado` e uma
`ReservaRecusada`, confirmados direto no `evento_do_estoque`:

```
SEQUENCIA=4 STREAM_ID=SHOW-PUCMINAS-2026::CAMAROTE VERSAO=2 TIPO=IngressoRetirado  DADOS={"quantidade":4,...}
SEQUENCIA=5 STREAM_ID=SHOW-PUCMINAS-2026::CAMAROTE VERSAO=3 TIPO=IngressoRetirado  DADOS={"quantidade":4,...}
SEQUENCIA=6 STREAM_ID=SHOW-PUCMINAS-2026::CAMAROTE VERSAO=4 TIPO=IngressoRetirado  DADOS={"quantidade":4,...}
SEQUENCIA=7 STREAM_ID=SHOW-PUCMINAS-2026::CAMAROTE VERSAO=5 TIPO=IngressoRetirado  DADOS={"quantidade":4,...}
SEQUENCIA=8 STREAM_ID=SHOW-PUCMINAS-2026::CAMAROTE VERSAO=6 TIPO=IngressoRetirado  DADOS={"quantidade":4,...}
SEQUENCIA=9 STREAM_ID=SHOW-PUCMINAS-2026::CAMAROTE VERSAO=7 TIPO=ReservaRecusada   DADOS={"quantidadePedida":4,"disponivelNoMomento":0,...}
```

E a projeção acompanhou: `disponibilidade_por_setor` fechou com `CAMAROTE: capacidade=20, retirados=20, disponivel=0`,
e `projecao_checkpoint` avançou até a sequência 9. Foi nesse teste real que o bug do deserializer do Kafka (acima)
apareceu e foi corrigido.

---

## Quem fez o quê

| Integrante                         | O que fez                                                                                                                                                                                                                                                                                                     |
|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Pedro Assis Corrêa                 | ADR-005; event store (`evento_do_estoque`, versão como detector de concorrência); agregado `EstoqueDoSetor` e os quatro eventos; a suíte de testes de replay, idempotência e concorrência; correção dos bloqueadores de build herdados da aula 02; esta folha de entrega.                                     |
| Amir Gabriel Dantas Santos Andrade | ADR-006; implementação da Projeção de Disponibilidade por Setor (`disponibilidade_por_setor`); repositório de checkpoint (`projecao_checkpoint`); motor de replay, loteamento e agendamento assíncrono (`ReconstrucaoService`); e a suíte de testes de integração da projeção (`ReconstrucaoDeProjecaoTest`). |
| Maria Luísa Lacerda                | Correções em [`docs/contrato.md`](../contrato.md): documentação do evento de reserva liberada e ajustes no contrato do evento de ingresso reservado.                                                                                                                                                        |
| Gabriel Campos Ferreira Lisboa     | correção do nome do arquivo `ADR-006` e de 18 links quebrados em `aula-05.md`; verificação por testes automatizados e execução manual dos dois serviços com Kafka real, que revelou e corrigiu um `IngressoListener` que não processava nenhuma reserva em runtime (o deserializer configurado não convertia a mensagem — nenhum teste automatizado cobria esse caminho); criação da tag `entrega-aula-05`.                                                                                                                                                    |

Os demais integrantes da equipe não têm commit registrado nesta etapa.
