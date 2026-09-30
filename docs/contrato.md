# Contrato do Evento — Ingresso Reservado

## 1. Identificação do evento

**Tipo do evento:** `vendas.ingresso.reservado.v1`

O evento representa a ocorrência de uma reserva de ingressos aceita pelo domínio. Ele é publicado pelo `servico-vendas` após a validação dos dados da reserva, incluindo o limite de ingressos por CPF e a disponibilidade do setor.

O evento representa um **fato ocorrido no domínio**, e não um comando. A publicação informa aos demais serviços que uma reserva foi realizada.

O evento é utilizado no fluxo de integração entre os serviços por meio do Kafka.

O campo `eventoId` identifica de forma única a ocorrência do evento e também é utilizado pelo consumidor para controle de idempotência e deduplicação.

---

## 2. Estrutura do evento

O evento utiliza **CloudEvents 1.0 em Binary Content Mode**, transportado por Kafka.

No modo binário, os atributos do CloudEvents são enviados como headers da mensagem Kafka, utilizando o prefixo `ce_`. O conteúdo do atributo `data` contém o payload JSON do evento.

### 2.1 Payload `data`

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `eventoId` | `string (UUID)` | Sim | Identificador único da ocorrência do evento. É utilizado também para controle de idempotência e deduplicação. |
| `compraId` | `string` | Sim | Identificador da compra associada à reserva. |
| `cpfComprador` | `string` | Sim | CPF do comprador responsável pela reserva. |
| `evento` | `string` | Sim | Identificador do evento para o qual os ingressos foram reservados. |
| `itens` | `array<ItemDoIngressoVO>` | Sim | Lista dos itens de ingresso reservados. |
| `reservadoEm` | `string (ISO-8601 date-time)` | Sim | Data e hora em que a reserva ocorreu no domínio. |

### 2.2 `ItemDoIngressoVO`

Cada item da lista `itens` possui a seguinte estrutura:

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `setor` | `string` | Sim | Setor do evento ao qual o ingresso pertence. |
| `quantidade` | `integer` | Sim | Quantidade de ingressos reservados. Deve ser maior que zero. |
| `precoUnitario` | `number` | Sim | Preço unitário do ingresso no momento da reserva. |

Os campos obrigatórios do `ItemDoIngressoVO` não podem ser nulos. A quantidade deve ser maior que zero.

---

## 3. Datas e horários

O campo `reservadoEm` representa a data e hora em que o fato de negócio ocorreu, ou seja, quando a reserva foi realizada no domínio.

O valor é representado utilizando `Instant` e serializado como texto no formato **ISO-8601**. O contrato não utiliza representação numérica de timestamp em epoch.

Exemplo:

```text
2026-08-18T20:30:00Z
```

Essa informação representa o momento da ocorrência da reserva e não o momento em que a mensagem foi recebida pelo broker ou processada pelo consumidor.

---

## 4. Chave de particionamento

A chave de particionamento utilizada no Kafka é o campo `evento`.

Dessa forma, reservas referentes ao mesmo evento de entretenimento são direcionadas para a mesma partição Kafka, mantendo a ordenação das mensagens que possuem a mesma chave.

Essa decisão é importante porque diferentes reservas de um mesmo evento podem disputar a disponibilidade dos mesmos setores.

A ordenação garantida é relativa às mensagens que possuem a mesma chave de partição. Não existe garantia de ordenação global entre mensagens distribuídas em partições diferentes.

---

## 5. Atributos CloudEvents

O evento utiliza o padrão **CloudEvents 1.0**, no modo **Binary Content Mode**.

Os atributos do CloudEvents são transportados nos headers da mensagem Kafka utilizando o prefixo `ce_`.

Os atributos obrigatórios utilizados pelo contrato são:

| Atributo CloudEvents | Header Kafka | Descrição |
|---|---|---|
| `specversion` | `ce_specversion` | Versão da especificação CloudEvents utilizada. Deve ser `1.0`. |
| `id` | `ce_id` | Identificador da ocorrência do evento. Corresponde ao `eventoId` do payload. |
| `source` | `ce_source` | Identifica a origem responsável pela produção do evento. |
| `type` | `ce_type` | Tipo do evento. Deve ser `vendas.ingresso.reservado.v1`. |
| `time` | `ce_time` | Data e hora da ocorrência do fato de negócio, em formato ISO-8601. |

O atributo `ce_time` representa o momento em que a reserva ocorreu no domínio. Ele não representa o momento em que o broker Kafka recebeu a mensagem nem o momento em que o consumidor iniciou o processamento.

---

## 6. Compatibilidade

O contrato adota uma estratégia de compatibilidade **BACKWARD**.

Isso significa que alterações compatíveis devem permitir que consumidores que já estão em produção continuem processando eventos produzidos pela versão anterior.

Por exemplo, a inclusão de novos campos no payload pode ser realizada de forma compatível quando o consumidor não depende desses campos para processar o evento. O consumidor utiliza desserialização tolerante a propriedades desconhecidas.

Quando houver alteração incompatível no significado ou na estrutura dos dados, deve ser considerada uma nova versão do contrato, evitando alterar silenciosamente o significado de um campo já utilizado pelos consumidores.

Em uma evolução do contrato, recomenda-se que o consumidor seja disponibilizado antes do produtor quando isso reduzir a janela de incompatibilidade entre as versões.

Como os serviços deste projeto fazem parte da mesma solução, o time consegue coordenar a evolução dos produtores e consumidores. Caso novos consumidores de outros sistemas passem a depender do evento, alterações incompatíveis devem considerar uma estratégia de migração ou uma nova versão do evento.

---

## 7. Evolução do contrato

Alterações no contrato devem considerar não apenas o tipo do campo, mas também seu significado para o domínio.

Uma alteração pode ser incompatível mesmo mantendo o mesmo tipo de dado.

Por exemplo, alterar o significado de `precoUnitario` para representar o preço total dos ingressos seria uma mudança semântica incompatível, mesmo que ambos continuem sendo representados como `number`.

Alterações desse tipo devem ser tratadas como evolução do contrato e, quando necessário, resultar em uma nova versão do tipo do evento.

---

## 8. Exemplo do evento `IngressoReservadoEvent`

Exemplo fictício de payload:

```json
{
  "eventoId": "550e8400-e29b-41d4-a716-446655440000",
  "compraId": "compra-1001",
  "cpfComprador": "00000000000",
  "evento": "SHOW-2026-001",
  "itens": [
    {
      "setor": "PISTA",
      "quantidade": 2,
      "precoUnitario": 150.00
    },
    {
      "setor": "CAMAROTE",
      "quantidade": 1,
      "precoUnitario": 300.00
    }
  ],
  "reservadoEm": "2026-08-18T20:30:00Z"
}
```

Os dados apresentados são fictícios e servem apenas para demonstrar a estrutura do contrato.

---

# Contrato do Evento — Ingresso Liberado

## 9. Identificação do evento

**Tipo do evento:** `vendas.ingresso.liberado.v1`

O evento representa o fato de que os ingressos retirados por uma reserva voltaram a ficar disponíveis para venda porque o pagamento da compra foi recusado.

O `IngressoLiberadoEvent` é publicado pelo `servico-vendas` depois que o serviço consome o `PagamentoRecusadoEvent` correspondente à compra e decide liberar a reserva.

Esse evento representa um **fato ocorrido no domínio**, e não um comando. O `servico-vendas` informa que a liberação ocorreu; cabe ao `servico-ingressos`, a partir do seu próprio histórico, determinar exatamente quais quantidades foram retiradas pela reserva e devem retornar ao estoque.

O campo `eventoId` identifica de forma única a ocorrência da liberação e também é utilizado para deduplicação. O campo `reservaEventoId` referencia o `eventoId` do `IngressoReservadoEvent` que está sendo desfeito. O campo `pagamentoEventoId` referencia o `eventoId` do `PagamentoRecusadoEvent` que causou a liberação.

---

## 10. Estrutura do evento

O evento utiliza **CloudEvents 1.0 em Binary Content Mode**, transportado por Kafka.

Os atributos do CloudEvents são enviados como headers da mensagem Kafka utilizando o prefixo `ce_`, enquanto o atributo `data` contém o payload JSON.

### 10.1 Payload `data`

| Campo | Tipo | Obrigatório | Significado |
|---|---|---|---|
| `eventoId` | `string (UUID)` | Sim | Identidade desta liberação e chave de deduplicação. Igual ao `ce_id`. |
| `compraId` | `string` | Sim | Compra cuja reserva foi liberada. Serve para correlacionar logs; o estoque não usa. |
| `reservaEventoId` | `string (UUID)` | Sim | `eventoId` do `IngressoReservadoEvent` desfeito. É por ele que o estoque encontra o que a reserva retirou. |
| `pagamentoEventoId` | `string (UUID)` | Sim | `eventoId` do `PagamentoRecusadoEvent` que causou a liberação. |
| `evento` | `string` | Sim | Evento de entretenimento. É a chave de partição. |
| `itens` | `array<ItemDoIngressoVO>` | Sim | Itens da reserva original. O estoque usa somente os setores. |
| `motivo` | `string` | Sim | Motivo da recusa, em texto livre. |
| `liberadoEm` | `string (ISO-8601 date-time)` | Sim | Instante em que o `servico-vendas` decidiu liberar. |

### 10.2 `ItemDoIngressoVO`

Cada item da lista `itens` possui a seguinte estrutura:

| Campo | Tipo | Obrigatório | Significado |
|---|---|---|---|
| `setor` | `string` | Sim | Setor do evento ao qual o ingresso pertence. |
| `quantidade` | `integer` | Sim | Quantidade da reserva original. É informativa para a liberação; o estoque devolve a quantidade efetivamente registrada como retirada pela reserva. |
| `precoUnitario` | `number` | Sim | Preço unitário do ingresso associado ao item. |

O `IngressoLiberadoEvent` mantém os itens da reserva para preservar o contexto do fato. A quantidade recebida, entretanto, não é a fonte da verdade para a devolução. O `servico-ingressos` usa `reservaEventoId` para consultar o histórico do estoque e determinar quanto aquela reserva efetivamente retirou de cada setor.

---

## 11. Exemplo do evento `IngressoLiberadoEvent`

Exemplo fictício de payload:

```json
{
  "eventoId": "660f9511-f3ac-52e5-b827-557766551111",
  "compraId": "compra-1001",
  "reservaEventoId": "550e8400-e29b-41d4-a716-446655440000",
  "pagamentoEventoId": "770a0622-04bd-43f6-8938-668877662222",
  "evento": "SHOW-2026-001",
  "itens": [
    {
      "setor": "PISTA",
      "quantidade": 2,
      "precoUnitario": 150.00
    },
    {
      "setor": "CAMAROTE",
      "quantidade": 1,
      "precoUnitario": 300.00
    }
  ],
  "motivo": "pagamento recusado pelo gateway (simulado)",
  "liberadoEm": "2026-08-18T20:45:00Z"
}
```

O `reservaEventoId` deste exemplo é o `eventoId` da reserva apresentada na seção 8. O `pagamentoEventoId` corresponde ao evento de pagamento recusado que originou a liberação.

Os dados apresentados são fictícios e servem apenas para demonstrar a estrutura do contrato.

---

## 12. Atributos CloudEvents da liberação

O evento de liberação utiliza **CloudEvents 1.0**, no modo **Binary Content Mode**.

Os atributos são transportados nos headers Kafka:

| Atributo CloudEvents | Header Kafka | Descrição |
|---|---|---|
| `specversion` | `ce_specversion` | Versão da especificação CloudEvents utilizada. Deve ser `1.0`. |
| `id` | `ce_id` | Identificador da ocorrência da liberação. Corresponde ao `eventoId` do payload. |
| `source` | `ce_source` | Identifica o `servico-vendas` como origem responsável pela produção do evento. |
| `type` | `ce_type` | Tipo do evento. Deve ser `vendas.ingresso.liberado.v1`. |
| `time` | `ce_time` | Instante da liberação, correspondente a `liberadoEm`, em formato ISO-8601. |

A chave Kafka utilizada é o campo `evento`, mantendo os fatos relativos ao mesmo evento de entretenimento na mesma partição.

---

## 13. Compatibilidade e evolução da liberação

O tipo `vendas.ingresso.liberado.v1` substitui `vendas.ingresso.reserva-compensada.v1`.

O nome anterior descrevia o mecanismo de compensação, enquanto o novo tipo descreve o fato de domínio observado pelos consumidores: os ingressos da reserva foram liberados.

Além da mudança semântica, o novo contrato introduz `reservaEventoId` como referência obrigatória à reserva que precisa ser desfeita e `pagamentoEventoId` como referência ao pagamento recusado que originou a liberação. Essas referências fazem parte da correlação explícita da Saga.

A introdução de campos obrigatórios que os consumidores anteriores não conheciam constitui uma alteração incompatível. Por isso a mudança é representada por um novo tipo de evento, e não por uma alteração silenciosa do contrato `vendas.ingresso.reserva-compensada.v1`.

A evolução do novo contrato continua seguindo a estratégia de compatibilidade **BACKWARD** para mudanças compatíveis.

---

## 14. Idempotência

Os consumidores devem considerar que uma mesma mensagem pode ser recebida mais de uma vez.

O `eventoId` identifica a ocorrência da liberação e participa do controle de deduplicação. Uma nova entrega do mesmo `IngressoLiberadoEvent` não deve produzir um novo efeito.

A proteção não se limita ao `eventoId`. O `reservaEventoId` também participa da idempotência no agregado `EstoqueDoSetor`: duas liberações diferentes referentes à mesma reserva devem devolver os ingressos apenas uma vez.

Assim, existem duas situações distintas protegidas pelo modelo:

- a repetição do mesmo evento é reconhecida pelo `eventoId`;
- eventos de liberação diferentes para a mesma reserva são reconhecidos pelo `reservaEventoId`.

O registro de processamento e o efeito de negócio devem ser realizados de forma consistente. O reconhecimento (`ack`) da mensagem ocorre somente depois que o processamento correspondente é concluído com sucesso.

---

## 15. Contrato do evento `PagamentoRecusadoEvent`

### 15.1 Identificação

**Tipo do evento:** `pagamentos.pagamento.recusado.v1`

O evento representa o fato de que o gateway de pagamento simulado recusou o pagamento associado a uma compra.

Ele é publicado pelo gateway de pagamento simulado, executado no `servico-vendas`, e consumido pelo próprio `servico-vendas` no grupo `servico-vendas`.

A chave Kafka é `compraId`.

### 15.2 Estrutura

| Campo | Tipo | Obrigatório | Significado |
|---|---|---|---|
| `eventoId` | `string (UUID)` | Sim | Identidade da ocorrência da recusa. Igual ao `ce_id`. |
| `compraId` | `string` | Sim | Compra cujo pagamento foi recusado. Também é a chave Kafka. |
| `motivo` | `string` | Sim | Motivo informado pelo gateway simulado. |
| `recusadoEm` | `string (ISO-8601 date-time)` | Sim | Instante em que o pagamento foi recusado. |

### 15.3 Atributos CloudEvents

O evento utiliza **CloudEvents 1.0 em Binary Content Mode**.

Os principais atributos são:

| Atributo CloudEvents | Valor |
|---|---|
| `specversion` | `1.0` |
| `id` | mesmo valor de `eventoId` |
| `source` | `gateway-de-pagamento-simulado` |
| `type` | `pagamentos.pagamento.recusado.v1` |
| `time` | mesmo instante de `recusadoEm` |

### 15.4 Exemplo

```json
{
  "eventoId": "770a0622-04bd-43f6-8938-668877662222",
  "compraId": "compra-1001",
  "motivo": "pagamento recusado pelo gateway (simulado)",
  "recusadoEm": "2026-08-18T20:44:58Z"
}
```

Os dados apresentados são fictícios e servem apenas para demonstrar a estrutura do contrato.

---

# Contrato das filas de mensagens mortas

## 16. Tópicos de dead letter

### 16.1 Nome e dono

Cada tópico consumido tem uma DLQ, com o nome do tópico original acrescido de `.dlq`. A DLQ é declarada pelo serviço que escreve nela.

| DLQ | Tópico original | Quem escreve | Grupos que podem desistir |
|---|---|---|---|
| `vendas.ingresso.reservado.v1.dlq` | `vendas.ingresso.reservado.v1` | `servico-ingressos` | `servico-ingressos` e `servico-ingressos-agregador-reservas` |
| `vendas.ingresso.liberado.v1.dlq` | `vendas.ingresso.liberado.v1` | `servico-ingressos` | `servico-ingressos` |
| `pagamentos.pagamento.recusado.v1.dlq` | `pagamentos.pagamento.recusado.v1` | `servico-vendas` | `servico-vendas` |

Cada DLQ tem uma partição e retenção de 30 dias. A partição de destino é escolhida pela chave da mensagem original.

A DLQ é lida por pessoas, e não por serviços. Nenhum consumidor da aplicação assina uma DLQ. A leitura e o reprocessamento passam por `GET /reprocessamentos/retidos` e `POST /reprocessamentos`, no `servico-ingressos`.

### 16.2 Valor e cabeçalhos

A chave e o valor são os mesmos que chegaram ao tópico original. O valor vai como texto UTF-8.

| Cabeçalho | Quem põe | Formato | Significado |
|---|---|---|---|
| `ce_specversion`, `ce_id`, `ce_source`, `ce_type`, `ce_time` | publicador original | texto UTF-8 | cópia exata dos atributos CloudEvents. O `ce_id` continua sendo a chave de deduplicação |
| `kafka_dlt-original-topic` | Spring Kafka | texto UTF-8 | tópico de onde a mensagem veio |
| `kafka_dlt-original-partition` | Spring Kafka | `int` de 4 bytes, big-endian | partição de origem |
| `kafka_dlt-original-offset` | Spring Kafka | `long` de 8 bytes, big-endian | offset de origem |
| `kafka_dlt-original-timestamp` | Spring Kafka | `long` de 8 bytes, big-endian | timestamp que o Kafka atribuiu à mensagem original, em milissegundos |
| `kafka_dlt-original-consumer-group` | Spring Kafka | texto UTF-8 | grupo que desistiu |
| `kafka_dlt-exception-fqcn`, `kafka_dlt-exception-cause-fqcn`, `kafka_dlt-exception-message`, `kafka_dlt-exception-stacktrace` | Spring Kafka | texto UTF-8 | a exceção como o Spring a recebeu |
| `dlq_excecao` | serviço | texto UTF-8 | classe da exceção que causou a falha, sem o embrulho do Spring |
| `dlq_mensagem` | serviço | texto UTF-8 | mensagem da exceção, com até 500 caracteres |
| `dlq_classificacao` | serviço | `permanente` ou `transitoria-esgotada` | se a falha é permanente ou se as retentativas acabaram |
| `dlq_falhou_em` | serviço | ISO-8601 | instante em que a mensagem foi para a DLQ |
| `dlq_servico` | serviço | texto UTF-8 | serviço que desistiu |

### 16.3 Reprocessamento

Ao reprocessar, o `servico-ingressos` republica a mensagem no tópico indicado por `kafka_dlt-original-topic`, com a mesma chave, o mesmo valor e os mesmos `ce_*`. Os cabeçalhos `kafka_dlt-*` e `dlq_*` saem, e entram dois:

| Cabeçalho | Formato | Significado |
|---|---|---|
| `reprocessado_em` | ISO-8601 | instante da republicação |
| `reprocessado_de` | `<topico-dlq>/<particao>/<offset>` | posição da mensagem na DLQ |

Os consumidores ignoram esses dois cabeçalhos. Como o `ce_id` não muda, um grupo que já tinha processado o evento o descarta pela própria deduplicação.