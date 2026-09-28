# ADR-006: resiliência, caminho de falha e a Saga do pagamento recusado

## Status

Proposta · 2026-09-25 · Equipe 01

Autoria: Pedro Assis Corrêa (256357) nas seções 1 a 4, 7 e 8; Maria Luísa Lacerda (257115) nas seções 5 e 6.

Relaciona-se com o ADR-002, o ADR-003, o ADR-005 e o ADR-007.

## Contexto

<!-- Pessoa A: tratador padrão do Spring Kafka e a plataforma Spring Boot 4.1. -->

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

Carga inválida, campo obrigatório ausente e regra violada vão direto para a DLQ. O resto é transitório.

### 2. Retentativa

Quatro retentativas na própria partição, com espera de 1, 2, 4 e 8 segundos.

### 3. Dead letter queue

Uma DLQ por tópico, com o nome do tópico original mais `.dlq`, os cabeçalhos CloudEvents originais e o motivo. Retenção de 30 dias.

### 4. Reprocessamento

Manual, por `ce_id`, por um endpoint do servico-ingressos, confiando na idempotência dos consumidores.

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

45 dias, mais que a retenção da DLQ.

### 8. Plataforma

Registro retroativo do uso de Spring Boot 4.1, Spring Kafka 4.1 e Jackson 3.

## Alternativas consideradas

<!-- Cada pessoa acrescenta as suas. -->

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

<!-- Cada pessoa acrescenta as suas. -->

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
