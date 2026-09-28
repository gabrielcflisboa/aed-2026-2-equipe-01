# ADR-006: resiliência, caminho de falha e a Saga do pagamento recusado

## Status

Aceita · 2026-09-28 · Equipe 01. Proposta em 2026-09-25.

Autoria: Pedro Assis Corrêa (256357) nas seções 1 a 4, 7 e 8; Maria Luísa Lacerda (257115) nas seções 5 e 6.

Relaciona-se com o ADR-002, o ADR-003, o ADR-005 e o ADR-007.

O caminho deste arquivo é fixo. O número 006 estava ocupado pelo ADR de
projeções, que passou a ser o ADR-007 sem mudança de conteúdo. A sugestão
inicial de numerar este ADR como 007 foi recusada, como registra o
`docs/IA.md`.

## Contexto

Até esta decisão, nenhum consumidor tinha tratador de erro configurado. Sem
um `CommonErrorHandler`, o Spring Kafka usa o padrão: entrega a mesma mensagem
dez vezes seguidas, sem espera entre as tentativas, escreve uma linha de log e
pula o registro. Um JSON malformado, uma quantidade zerada e um banco fora do
ar recebiam o mesmo tratamento, e nos três casos o evento sumia sem deixar
nada além do log. Não havia DLQ nem caminho para trazer o evento de volta.

Os tópicos têm uma partição cada, e reserva e liberação usam a chave `evento`.
O tópico de reservas é lido por dois grupos, o do estoque e o do agregador, e
cada um confirma o próprio offset. O agregador somava sem deduplicar.

Os dois serviços usam Spring Boot 4.1.0, Spring Kafka 4.1 e Jackson 3, e não
a versão 3.3.5 que serve de referência para os exemplos da disciplina. A
escolha foi feita na criação dos projetos e ficou sem registro até este ADR,
que a trata na seção 8. Uma consequência dela pesa aqui. O
`JacksonJsonDeserializer` não converteu as mensagens em execução real, como
registra `docs/entregas/aula-05.md`, e por isso os listeners recebem texto e
desserializam no próprio método. Um JSON inválido falha dentro do listener,
como `JacksonException`, e não no deserializador do consumidor.

A equipe tem seis pessoas, não mantém plantão, não tem ferramenta de
monitoração e roda um broker local com H2 embarcado. As decisões das seções
1 a 4, 7 e 8 foram escolhidas para funcionar nesse contexto.

Antes desta decisão, a compensação já era idempotente por `eventoId`, mas
nascia de um `POST /vendas/reservas/{compraId}/compensacoes`, e não da reação
a um fato de pagamento. O evento consumido pelo estoque também não identificava
qual reserva estava sendo desfeita. A devolução usava a quantidade informada
pela compensação e a comparava apenas com o total retirado do setor.

Isso permitia devolver ingressos que pertenciam a outra reserva. Por exemplo,
em uma PISTA com capacidade 10, a reserva A retira 8 ingressos. A reserva B
pede 5 e é recusada pelo estoque. Se depois o pagamento de B for recusado e
uma compensação de 5 for processada, o estoque poderia devolver esses 5 mesmo
sem B tê-los retirado. O setor passaria a mostrar 7 lugares livres apesar de
existirem 8 ingressos efetivamente vendidos.

Reserva e compensação também trafegavam em tópicos diferentes, sem garantia
de ordem entre elas. Uma compensação que chegasse antes da reserva
correspondente poderia lançar `IllegalStateException` ou devolver ingressos
retirados por outras reservas.

## Decisão

### 1. Falha transitória e falha permanente

A classificação mora no código, em `ResilienciaConfig`, em cada serviço. O
padrão é retentar. A lista explícita é a das falhas permanentes, que vão
direto para a DLQ porque a mesma mensagem falharia igual em qualquer
tentativa.

| Exceção | Onde nasce | Classe | Por quê |
|---|---|---|---|
| `JacksonException` e subclasses | `readValue` nos listeners | permanente | JSON malformado, campo obrigatório ausente ou quantidade menor ou igual a zero, validados no construtor dos eventos consumidos |
| `IllegalArgumentException` | eventos, agregado e agregador | permanente | regra violada, como `reservadoEm` ausente para o agregador |
| `NullPointerException` | defeito de código | permanente | retentar dá o mesmo erro; o evento volta pela DLQ depois do conserto |
| `CompraNaoEncontradaException` | `VendaService.liberarReserva` | permanente | o servico-vendas não conhece a compra |
| lista padrão do Spring Kafka | framework | permanente | falhas de conversão e de deserialização, que já vêm classificadas assim |
| `CannotGetJdbcConnectionException` e demais falhas de acesso ao H2 | repositórios | transitória | banco fora ou travado |
| `ConcorrenciaNoStreamException` | event store | transitória | outra gravação no mesmo stream; a tentativa seguinte relê o stream |
| `ReservaAindaNaoProcessadaException` | `EstoqueDoSetor.liberar` | transitória | a liberação chegou antes da reserva |
| `KafkaException` na confirmação do envio | `VendaLiberacaoCallbackService` | transitória | broker fora |
| qualquer outra | qualquer lugar | transitória | na dúvida, retenta com limite e termina na DLQ |

O classificador percorre a cadeia de causas. Uma `JacksonException`
embrulhada pelo Spring em `ListenerExecutionFailedException` continua
permanente.

### 2. Retentativa

Quatro retentativas na própria partição, depois da primeira entrega, com
espera exponencial de 1, 2, 4 e 8 segundos: 15 segundos no total.

Os números saem das falhas transitórias que este sistema tem. Uma queda de
conexão com o H2 e um conflito de versão no stream se resolvem em segundos.
Uma liberação que chega antes da reserva costuma encontrar a reserva
processada já na primeira retentativa, porque a reserva leva menos de um
segundo para ser processada. Quinze segundos é o maior tempo que a equipe
aceita com a partição parada. No servico-vendas, cada tentativa espera até 10
segundos pela confirmação do broker, o que dá no pior caso 65 segundos por
mensagem, abaixo dos 300 segundos do `max.poll.interval.ms`.

### 3. Dead letter queue

Quando a falha é permanente, ou quando as retentativas acabam, o tratador
publica a mensagem em `<tópico original>.dlq`. Há uma DLQ por tópico
consumido: `vendas.ingresso.reservado.v1.dlq` e
`vendas.ingresso.liberado.v1.dlq`, escritas pelo servico-ingressos, e
`pagamentos.pagamento.recusado.v1.dlq`, escrita pelo servico-vendas. Os grupos
de um mesmo tópico compartilham a DLQ, e o cabeçalho
`kafka_dlt-original-consumer-group` diz qual grupo desistiu.

A mensagem vai com o valor original e com os cabeçalhos `ce_*` intactos,
inclusive o `ce_id`, que é a chave de deduplicação dos consumidores. O Spring
acrescenta os `kafka_dlt-*`, com tópico, partição, offset, grupo e exceção de
origem. O `ResilienciaConfig` acrescenta cinco cabeçalhos com o motivo:
`dlq_excecao`, `dlq_mensagem`, `dlq_classificacao`, que vale `permanente` ou
`transitoria-esgotada`, `dlq_falhou_em`, em ISO-8601, e `dlq_servico`. O offset
só é confirmado depois que o broker confirma a gravação na DLQ, com
`ack-mode: manual_immediate` e `setCommitRecovered(true)`.

As DLQs guardam mensagens por 30 dias. A equipe, em rodízio semanal, consulta
as três todo dia útil pela manhã com `GET /reprocessamentos/retidos`. Mensagem
nas DLQs de liberação ou de pagamento é tratada no mesmo dia, porque é
ingresso preso para um pagamento recusado. O sinal que importa é a quantidade
de mensagens novas desde a consulta anterior, e não o total acumulado. Os 30
dias cobrem feriados e a ausência de quem estiver no rodízio.

### 4. Reprocessamento

O reprocessamento é manual e tem quatro passos. Se nada mudou desde a falha,
não se reprocessa.

1. `GET /reprocessamentos/retidos?topicoDlq=...` lista o que está na DLQ, com
   o motivo, o grupo que desistiu e a posição original.
2. A equipe corrige a causa: código, dado ou contrato.
3. `POST /reprocessamentos` recebe o tópico da DLQ e os `ce_id` escolhidos e
   republica cada um no tópico original, sem os cabeçalhos da DLQ e com
   `reprocessado_em` e `reprocessado_de`. Não existe opção "tudo", e cada
   `ce_id` é republicado uma vez por chamada, mesmo quando a DLQ guarda uma
   cópia por grupo.
4. Os consumidores descartam o que já processaram: o estoque por
   `evento_processado`, o agregador por `evento_agregado` e a liberação também
   pela `reservaEventoId`.

Republicar manda o evento de novo para o tópico original, e todos os grupos
daquele tópico o recebem outra vez. Por isso todo consumidor desses tópicos é
idempotente, e o agregador ganhou `evento_agregado`. O endpoint fica no
servico-ingressos e atende também a DLQ do servico-vendas, porque só lê Kafka
e republica, sem conhecer o evento.

### 5. A Saga do pagamento recusado

O caminho de exceção começa quando o pagamento de uma reserva aceita é
recusado depois que o estoque já retirou os ingressos. Nesse caso, é necessário
devolver ao setor exatamente o que aquela reserva retirou.

A Saga é coreografada em três passos:

1. O `servico-vendas` publica `IngressoReservadoEvent` em
   `vendas.ingresso.reservado.v1`, com chave `evento`, e o
   `servico-ingressos` processa a retirada.
2. O gateway de pagamento simulado publica `PagamentoRecusadoEvent` em
   `pagamentos.pagamento.recusado.v1`, com chave `compraId`. O
   `servico-vendas` reage ao fato e localiza a reserva correspondente.
3. O `servico-vendas` publica `IngressoLiberadoEvent` em
   `vendas.ingresso.liberado.v1`, com chave `evento`. O
   `servico-ingressos` reage à liberação e devolve ao setor exatamente o que
   a reserva identificada por `reservaEventoId` havia retirado.

Foi escolhida coreografia porque há três participantes e um passo de
compensação. Cada participante reage a um fato e confirma o próprio efeito,
sem introduzir um componente central responsável por armazenar o estado da
Saga.

### 6. Quando a própria compensação falha

A liberação também pode falhar. Como reserva e liberação trafegam em tópicos
diferentes, `IngressoLiberadoEvent` pode chegar ao `servico-ingressos` antes
do `IngressoReservadoEvent` que pretende desfazer. Nesse caso,
`ReservaAindaNaoProcessadaException` representa uma falha transitória: a
liberação pode ser tentada novamente depois que a reserva for processada.

Se as tentativas forem esgotadas, a liberação segue para
`vendas.ingresso.liberado.v1.dlq`. Depois que a reserva correspondente estiver
disponível no stream, a liberação pode ser reprocessada.

A tentativa que falha não pode deixar o evento marcado como processado. Quando
o reprocessamento finalmente tiver sucesso, o agregado usa `reservaEventoId`
para garantir que aquela reserva seja liberada uma única vez. Assim, inclusive
duas liberações com `eventoId` diferentes para a mesma reserva produzem no
máximo um `IngressoDevolvidoEvent`.

### 7. Retenção da deduplicação

`evento_processado` e `evento_agregado` guardam cada `eventoId` por 45 dias.
A regra é que a memória de entrega dure mais que a DLQ, que guarda 30 dias, e
mais que o tópico, que guarda 7. Um evento pode ficar 30 dias na DLQ, e um
grupo pode tê-lo processado no dia zero enquanto outro falhou. Quando ele for
reprocessado no dia 29, o grupo que já o processou precisa se lembrar dele. O
`RetencaoDaDeduplicacaoService` apaga todo dia, às 3h, o que passou dos 45
dias. A decisão responde por quanto tempo um `eventoId` precisa ser lembrado,
pergunta que estava aberta desde a primeira versão do consumidor idempotente.

### 8. Plataforma

A equipe mantém Spring Boot 4.1.0, Spring Kafka 4.1 e Jackson 3 e registra a
escolha aqui, depois do fato. O que ela muda no código: os pacotes do Jackson
são `tools.jackson.*`, com as anotações ainda em
`com.fasterxml.jackson.annotation.*`; o produtor usa `JacksonJsonSerializer`;
`send()` devolve `CompletableFuture`; e os listeners desserializam o JSON no
próprio método. Voltar para a 3.3.5 a esta altura custaria reescrever os dois
serviços sem ganho de funcionalidade.

## Alternativas consideradas

Na parte referente ao caminho de falha, foram consideradas e descartadas:

- **`try/catch` no listener com log:** o offset anda, o painel fica verde e o
  evento some sem rastro.
- **Retentar sem limite:** um evento que nunca vai passar prende a partição
  para sempre, e o lag cresce sem erro novo no log.
- **Tópicos de retentativa em degraus, com `@RetryableTopic`:** liberam a
  partição durante a espera, mas quebram a ordem por chave e custam um tópico
  e um consumidor por degrau. As falhas transitórias deste sistema se
  resolvem em segundos, e a espera na própria partição basta.
- **Reprocessamento automático, com um consumidor que republica a DLQ:** uma
  falha permanente voltaria para a DLQ em laço, e ninguém decidiria se a
  causa foi corrigida.
- **Uma DLQ por grupo:** o nome deixaria de derivar só do tópico original, e o
  cabeçalho `kafka_dlt-original-consumer-group` já identifica o grupo.
- **Ferramenta de reprocessamento em cada serviço, ou um serviço só para
  operação:** duplicaria o mesmo código nos dois projetos ou criaria um
  terceiro serviço fora da estrutura do repositório.
- **`ErrorHandlingDeserializer`:** trata falhas no deserializador do
  consumidor, que aqui não acontecem, porque o JSON é lido dentro do listener.
- **Guardar a deduplicação para sempre:** a tabela cresceria sem limite, sem
  que ninguém tivesse decidido por quanto tempo um `eventoId` precisa ser
  lembrado.

Na parte referente à Saga, foram consideradas e descartadas:

- **Orquestrador com o estado de cada compra:** acrescentaria um componente
  central e persistência para coordenar um fluxo que possui três participantes
  e um único passo de compensação.
- **Chamada REST do `servico-vendas` ao `servico-ingressos` com rollback:**
  transformaria a compensação em uma transação distribuída com outro nome,
  acoplando a disponibilidade dos dois serviços.
- **Apagar `IngressoRetiradoEvent` ou atualizar diretamente a projeção:**
  contrariaria a decisão de event sourcing do ADR-005, pois o fato ocorrido
  deixaria de fazer parte do histórico ou a projeção passaria a ser usada como
  fonte da verdade.
- **Reserva e liberação no mesmo tópico para obter ordem:** todo consumidor de
  reservas passaria a receber também liberações. Além disso, uma reserva
  enviada à DLQ e posteriormente republicada voltaria no fim do log, podendo
  aparecer depois da própria liberação.
- **Devolver a quantidade informada em `IngressoLiberadoEvent`:** permitiria
  devolver uma quantidade diferente daquela efetivamente retirada pela reserva.
  O histórico do agregado é a fonte usada para determinar a devolução.
- **Encontrar a reserva pelo `compraId` no estoque:** o log do estoque não
  conhece `compraId`; a correlação necessária para desfazer a retirada é
  `reservaEventoId`.
- **Manter o tipo `reserva-compensada` e apenas acrescentar o novo campo:**
  manteria um nome associado ao mecanismo anterior e introduziria um campo
  obrigatório incompatível com os consumidores existentes.

## Consequências aceitas

Na parte referente ao caminho de falha, são aceitas as seguintes
consequências:

- Uma falha transitória segura a partição por até 15 segundos. Com uma
  partição e chave `evento`, as reservas de todos os shows param juntas nesse
  intervalo.
- São três tópicos a mais para operar e uma rotina diária para alguém
  cumprir. Se ninguém olhar em 30 dias, a mensagem expira e o ingresso preso
  continua preso, agora sem registro.
- Reprocessar quebra a ordem, porque o evento volta no fim do log. Uma
  reserva reprocessada pode ser recusada por falta de um estoque que existia
  no horário original. Reprocessar reserva é decisão de negócio, e não só de
  infraestrutura.
- Reprocessar entrega o evento a todos os grupos do tópico. Todo consumidor
  novo desses tópicos precisa nascer idempotente, e recomeçar o agregador do
  zero exige zerar a agregação e `evento_agregado` juntos.
- A DLQ de reservas guarda o `cpfComprador` por 30 dias, mais que os 7 do
  tópico original.
- A memória de deduplicação precisa durar mais que a DLQ. Se o expurgo parar,
  a tabela cresce; se alguém reduzir a retenção sem mexer na DLQ, um
  reprocessamento tardio aplica o efeito de novo.
- O endpoint de reprocessamento não tem autenticação. A idempotência limita o
  estrago, mas não o acesso.
- A ferramenta de reprocessamento mora no servico-ingressos e conhece o nome
  da DLQ do servico-vendas.
- O valor chega à DLQ como texto UTF-8. Um byte que não seja UTF-8 já teria
  sido trocado pelo `StringDeserializer` na leitura, e a DLQ não o
  preservaria.
- Material de referência escrito para Spring Boot 3 e sugestões de
  ferramentas de IA erram mais com a versão 4.1, como o `docs/IA.md`
  registra.

Na parte referente à Saga, são aceitas as seguintes consequências:

- O ingresso permanece indisponível entre a recusa do pagamento e a efetiva
  devolução ao estoque.
- O `servico-vendas` mantém reservas, cotas e compras liberadas em memória. Um
  reinício impede liberar reservas anteriores a ele. Uma recusa repetida depois
  do reinício pode ir para a DLQ como compra desconhecida mesmo quando a
  liberação já ocorreu.
- O `servico-vendas` deixa de atuar apenas como publicador: passa a possuir
  grupo de consumo, retentativa e DLQ que precisam ser operados.
- O gateway de pagamento simulado roda no mesmo processo que reage às recusas.
  Se o `servico-vendas` cair, gateway e consumidor caem juntos, comportamento
  diferente do esperado com uma operadora externa real.
