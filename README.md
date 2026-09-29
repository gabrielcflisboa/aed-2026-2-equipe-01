# Equipe 01 · AED 2026/2

## Integrantes

| Nome completo                          | Matrícula     |
| -------------------------------------- | ------------- |
| Gabriel Campos Ferreira Lisboa (líder) | `255696`      |
| Maria Luísa Lacerda                    | `257115 `     |
| Amir Gabriel Dantas Santos Andrade     | `1666035`     |
| Pedro Assis Corrêa                     | `256357`      |
| Thiago Felipe dos Santos               | `258087`      |
| Willian dos Santos Miranda             | `258173`      |

## Domínio

Venda de ingressos para eventos: reserva sujeita a limite por CPF e
disponibilidade do setor, pagamento externo simulado, e compensação
(liberação do ingresso) quando a reserva expira ou o pagamento é recusado.
Detalhes e critérios atendidos em
[docs/adr/ADR-002-dominio-do-projeto.md](docs/adr/ADR-002-dominio-do-projeto.md).

## Estrutura

- `servico-vendas` — publisher: recebe a solicitação de reserva, valida limite por CPF
  e disponibilidade, e publica `IngressoReservadoEvent`. Não persiste nada.
- `servico-ingressos` — consumer idempotente com **event sourcing**: o estoque não é
  uma tabela, é derivado de um log append-only (`evento_do_estoque`). O agregado é
  `EstoqueDoSetor`, um stream por `(evento, setor)`, e a versão do stream é o que
  detecta concorrência. Toda leitura do estoque é uma releitura do stream.
  Roda também, no mesmo processo, um segundo consumidor com `group.id` próprio
  (`AgregadorDeReservasListener`) que agrega reservas por setor/evento em janelas de
  1 minuto — ver [docs/entregas/aula-03.md](docs/entregas/aula-03.md).

Documentos:

- [ADR-002 — domínio do projeto](docs/adr/ADR-002-dominio-do-projeto.md)
- [ADR-003 — contrato, agregador e compensação](docs/adr/ADR-003-contrato-agregador-e-compensacao.md)
- [ADR-005 — event sourcing no estoque](docs/adr/ADR-005-event-sourcing.md)
- [Contrato do evento `IngressoReservadoEvent`](docs/contrato.md)
- [Entrega da aula 03](docs/entregas/aula-03.md) — agregador por janela de tempo
- [Entrega da aula 05](docs/entregas/aula-05.md) — como rodar e como conferir o log
- Padrões de pacote, nomenclatura e idempotência em [AGENTS.md](AGENTS.md)

## Como rodar (máquina limpa)

Pré-requisitos: JDK 21, Docker. O Maven é resolvido pelo wrapper (`mvnw`/`mvnw.cmd`), não precisa instalar globalmente.

1. Suba a infraestrutura (Kafka + Kafka UI):
   ```powershell
   docker compose up -d
   ```
2. Em um terminal, suba o consumidor:
   ```powershell
   cd servico-ingressos
   ./mvnw.cmd spring-boot:run
   ```
3. Em outro terminal, suba o publisher:
   ```powershell
   cd servico-vendas
   ./mvnw.cmd spring-boot:run
   ```
4. Dispare uma reserva de ingresso (ajustar payload/endpoint conforme `VendaController`):
   ```powershell
   curl -X POST http://localhost:8080/vendas/reservas -H "Content-Type: application/json" -d '@exemplo-reserva.json'
   ```
5. Acompanhe as mensagens e os cabeçalhos `ce_*` no Kafka UI: http://localhost:8081
6. Consulte a agregação por setor/evento (o `servico-ingressos` já processa as
   reservas nos dois consumidores assim que sobe, não precisa de passo extra):
   ```powershell
   Invoke-RestMethod "http://localhost:8082/agregacao/reservas-por-setor?evento=show-demo"
   ```
7. Simule a recusa do pagamento da compra que você acabou de reservar (devolve o
   estoque via evento de compensação — `compra-0001` é o `compraId` do
   `exemplo-reserva.json`):
   ```powershell
   curl -X POST http://localhost:8080/vendas/reservas/compra-0001/compensacoes
   ```

No arranque, o `servico-ingressos` abre os setores de `app.abertura` gravando um
`SetorAbertoEvent` no log de cada stream — não há tabela de estoque semeada por
`data.sql`, porque estado inserido direto na tabela não sobreviveria a um replay.

Para derrubar tudo (inclusive volumes):

```powershell
docker compose down -v
```

## Conferir o event store

```powershell
cd servico-ingressos
./mvnw.cmd test -Dtest=EventoDoEstoqueRepositoryTest
```

Os eventos saem do stream na ordem em que entraram, a versão é por stream e não
global, e duas gravações feitas sobre a mesma leitura colidem: a segunda vira
`ConcorrenciaNoStreamException`. É a versão detectando concorrência, sem lock. O
que olhar direto no banco está em
[docs/entregas/aula-05.md](docs/entregas/aula-05.md#conferir-o-log).

Com o `servico-ingressos` rodando, o histórico de um setor também aparece por HTTP,
com cada fato do log, a capacidade e o que ainda está disponível:

```powershell
Invoke-RestMethod http://localhost:8082/estoque/show-pucminas-2026/PISTA
```

## Quando algo falha: DLQ e reprocessamento

Uma mensagem que um consumidor não consegue processar é entregue de novo, até quatro
vezes, com espera de 1, 2, 4 e 8 segundos. Carga inválida não espera: vai direto para
a DLQ. Esgotadas as tentativas, a mensagem vai para a DLQ do tópico, com o nome do
tópico acrescido de `.dlq`, levando os cabeçalhos `ce_*` originais e o motivo da falha.
As decisões estão no [ADR-006](docs/adr/ADR-006-resiliencia.md), e o formato das DLQs
na seção 16 do [contrato](docs/contrato.md).

### Ver o que está na DLQ

```powershell
Invoke-RestMethod "http://localhost:8082/reprocessamentos/retidos?topicoDlq=vendas.ingresso.reservado.v1.dlq"
Invoke-RestMethod "http://localhost:8082/reprocessamentos/retidos?topicoDlq=vendas.ingresso.liberado.v1.dlq"
Invoke-RestMethod "http://localhost:8082/reprocessamentos/retidos?topicoDlq=pagamentos.pagamento.recusado.v1.dlq"
```

Cada entrada traz o `ceId`, o grupo que desistiu, a `excecao`, a `mensagem`, a
`classificacao` (`permanente` ou `transitoria-esgotada`), o instante `falhouEm` e a carga
original.

### Como reprocessar

1. Leia a DLQ com os comandos acima e entenda o motivo.
2. Corrija a causa: código, dado ou contrato. Se nada mudou, a mensagem falha de novo.
3. Republique os eventos escolhidos pelo `ce_id`:
   ```powershell
   Invoke-RestMethod -Method Post -Uri http://localhost:8082/reprocessamentos -ContentType 'application/json' -Body '{"topicoDlq":"vendas.ingresso.liberado.v1.dlq","ceIds":["<ce_id>"]}'
   ```
4. Confira o efeito com `GET /estoque`. Reprocessar o mesmo `ce_id` de novo não duplica
   nada: os consumidores descartam o que já processaram.

O evento republicado volta no fim do tópico original e chega de novo a todos os grupos
que leem aquele tópico.

### Roteiro: evento inválido vai para a DLQ

Os eventos de teste entram pelo `kafka-console-producer` do contêiner, no formato
`cabeçalhos#chave|valor`, com cada cabeçalho escrito como `nome:valor`:

```powershell
'ce_specversion:1.0,ce_id:7d0c1f2a-0000-4000-8000-000000000009,ce_source:roteiro-readme,ce_type:vendas.ingresso.reservado.v1,ce_time:2026-09-28T12:00:00Z#show-pucminas-2026|{ isto nao e json' | docker exec -i aed-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9094 --topic vendas.ingresso.reservado.v1 --property parse.headers=true --property 'headers.delimiter=#' --property parse.key=true --property 'key.separator=|'

Invoke-RestMethod "http://localhost:8082/reprocessamentos/retidos?topicoDlq=vendas.ingresso.reservado.v1.dlq"
```

Esperado: duas entradas com o mesmo `ceId`, uma para o grupo do estoque e outra para o do
agregador, as duas com `classificacao` igual a `permanente` e sem retentativa no log.

### Roteiro: liberação que chega antes da reserva

1. Publique a liberação de uma reserva que ainda não existe:
   ```powershell
   'ce_specversion:1.0,ce_id:7d0c1f2a-0000-4000-8000-000000000002,ce_source:roteiro-readme,ce_type:vendas.ingresso.liberado.v1,ce_time:2026-09-28T12:05:00Z#show-pucminas-2026|{"eventoId":"7d0c1f2a-0000-4000-8000-000000000002","compraId":"compra-roteiro-01","reservaEventoId":"7d0c1f2a-0000-4000-8000-000000000001","pagamentoEventoId":"7d0c1f2a-0000-4000-8000-000000000003","evento":"show-pucminas-2026","itens":[{"setor":"PISTA","quantidade":2,"precoUnitario":180.00}],"motivo":"pagamento recusado pelo gateway (simulado)","liberadoEm":"2026-09-28T12:05:00Z"}' | docker exec -i aed-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9094 --topic vendas.ingresso.liberado.v1 --property parse.headers=true --property 'headers.delimiter=#' --property parse.key=true --property 'key.separator=|'
   ```
2. Espere uns 20 segundos. O log do `servico-ingressos` mostra as retentativas, e a
   liberação aparece na DLQ com `classificacao` igual a `transitoria-esgotada` e `excecao`
   terminando em `ReservaAindaNaoProcessadaException`:
   ```powershell
   Invoke-RestMethod "http://localhost:8082/reprocessamentos/retidos?topicoDlq=vendas.ingresso.liberado.v1.dlq"
   ```
3. Publique a reserva:
   ```powershell
   'ce_specversion:1.0,ce_id:7d0c1f2a-0000-4000-8000-000000000001,ce_source:roteiro-readme,ce_type:vendas.ingresso.reservado.v1,ce_time:2026-09-28T12:00:00Z#show-pucminas-2026|{"eventoId":"7d0c1f2a-0000-4000-8000-000000000001","compraId":"compra-roteiro-01","cpfComprador":"000.000.000-00","evento":"show-pucminas-2026","itens":[{"setor":"PISTA","quantidade":2,"precoUnitario":180.00}],"reservadoEm":"2026-09-28T12:00:00Z"}' | docker exec -i aed-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9094 --topic vendas.ingresso.reservado.v1 --property parse.headers=true --property 'headers.delimiter=#' --property parse.key=true --property 'key.separator=|'
   ```
4. Reprocesse a liberação e confira o estoque:
   ```powershell
   Invoke-RestMethod -Method Post -Uri http://localhost:8082/reprocessamentos -ContentType 'application/json' -Body '{"topicoDlq":"vendas.ingresso.liberado.v1.dlq","ceIds":["7d0c1f2a-0000-4000-8000-000000000002"]}'
   Invoke-RestMethod http://localhost:8082/estoque/show-pucminas-2026/PISTA
   ```
   O histórico mostra o `IngressoRetirado` da reserva `...0001` e o `IngressoDevolvido`
   com `reservaEventoId` igual a `...0001`.
5. Repita o passo 4. O histórico continua com uma devolução só, e o log do
   `servico-ingressos` mostra `liberacao repetida ignorada`.

Os identificadores dos roteiros são fixos. Para repetir um roteiro, comece do zero ou
troque os identificadores, porque a deduplicação lembra deles por 45 dias.

### Começar do zero

```powershell
docker compose down -v
Remove-Item -Recurse -Force servico-ingressos\data
```

O primeiro comando apaga os tópicos, e o segundo apaga o banco H2 do `servico-ingressos`,
com o log do estoque e a memória de deduplicação.

## Testes

```powershell
cd servico-vendas;    ./mvnw.cmd test   # 19 testes
cd servico-ingressos; ./mvnw.cmd test   # 42 testes
```

O `CaminhoDeFalhaTest` sobe um Kafka embutido e prova o caminho de falha de ponta a ponta:
JSON inválido e quantidade zero vão direto para a DLQ com os `ce_*` originais e o motivo,
a partição segue processando, uma falha transitória é retentada sem chegar à DLQ, e
reprocessar duas vezes o mesmo evento produz o efeito uma vez, no estoque e no agregador.
