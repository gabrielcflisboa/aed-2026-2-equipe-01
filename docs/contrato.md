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

# Contrato do Evento — Reserva Compensada

## 9. Identificação do evento

**Tipo do evento:** `vendas.ingresso.reserva-compensada.v1`

O evento representa a ocorrência de uma compensação de reserva de ingressos.

No fluxo atual, a compensação informa ao `servico-ingressos` que as quantidades de ingressos associadas à reserva devem ser devolvidas ao estoque.

Assim como o evento de reserva, esse evento representa um **fato ocorrido no domínio**, e não um comando.

O campo `eventoId` identifica a ocorrência da reserva relacionada à compensação e é utilizado pelo consumidor para controle de idempotência e deduplicação.

> **Observação de modelagem:** o nome `reserva-compensada` será mantido neste contrato para refletir o nome atualmente utilizado pela implementação. O feedback sobre a possibilidade de adotar um nome orientado ao fato de negócio, como `ReservaCancelada` ou `IngressoReservaLiberado`, deve ser tratado como uma decisão de evolução do domínio e do contrato, pois uma alteração desse tipo também impactaria produtor, consumidor, tipo do CloudEvent e tópico Kafka.

---

## 10. Estrutura do evento

O evento utiliza **CloudEvents 1.0 em Binary Content Mode**, transportado por Kafka.

Os atributos do CloudEvents são enviados como headers da mensagem Kafka utilizando o prefixo `ce_`, enquanto o atributo `data` contém o payload JSON.

### 10.1 Payload `data`

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `eventoId` | `string (UUID)` | Sim | Identificador da ocorrência da reserva relacionada à compensação. Utilizado também para controle de idempotência e deduplicação. |
| `itens` | `array<ItemDoIngressoVO>` | Sim | Lista dos itens de ingresso cujas quantidades devem ser devolvidas ao estoque. |

### 10.2 `ItemDoIngressoVO`

Cada item da lista `itens` possui a seguinte estrutura:

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `setor` | `string` | Sim | Setor do evento ao qual o ingresso pertence. |
| `quantidade` | `integer` | Sim | Quantidade de ingressos a ser devolvida ao estoque. Deve ser maior que zero. |
| `precoUnitario` | `number` | Sim | Preço unitário do ingresso associado ao item. |

O evento de compensação utiliza a mesma estrutura de `ItemDoIngressoVO` utilizada pelo evento de reserva.

---

## 11. Exemplo do evento `IngressoReservaCompensadaEvent`

Exemplo fictício de payload:

```json
{
  "eventoId": "550e8400-e29b-41d4-a716-446655440000",
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
  ]
}
```

O `eventoId` deve utilizar o mesmo identificador da ocorrência da reserva que está sendo compensada.

Os dados apresentados são fictícios e servem apenas para demonstrar a estrutura do contrato.

---

## 12. Atributos CloudEvents da compensação

O evento de compensação utiliza **CloudEvents 1.0**, no modo **Binary Content Mode**.

Os atributos são transportados nos headers Kafka:

| Atributo CloudEvents | Header Kafka | Descrição |
|---|---|---|
| `specversion` | `ce_specversion` | Versão da especificação CloudEvents utilizada. Deve ser `1.0`. |
| `id` | `ce_id` | Identificador da ocorrência do evento de compensação. |
| `source` | `ce_source` | Identifica a origem responsável pela produção do evento. |
| `type` | `ce_type` | Tipo do evento. Deve ser `vendas.ingresso.reserva-compensada.v1`. |
| `time` | `ce_time` | Data e hora da ocorrência do fato de negócio, em formato ISO-8601. |

O `ce_id` identifica o evento de compensação produzido, enquanto o `eventoId` no payload identifica a ocorrência da reserva relacionada à compensação. Esses identificadores não devem ser tratados como necessariamente equivalentes.

---

## 13. Compatibilidade da compensação

O contrato do evento de compensação também adota uma estratégia de compatibilidade **BACKWARD**.

Novos campos podem ser adicionados ao payload desde que não alterem o significado dos campos existentes e que os consumidores continuem capazes de processar eventos produzidos por versões anteriores.

Alterações incompatíveis na estrutura ou no significado dos dados devem resultar em uma estratégia explícita de evolução do contrato, podendo envolver uma nova versão do tipo do evento.

---

## 14. Idempotência

Os consumidores dos eventos devem considerar a possibilidade de recebimento de uma mesma mensagem mais de uma vez.

No caso do evento de reserva, o `eventoId` é utilizado como identificador da ocorrência para controle de eventos já processados.

No caso da compensação, o `eventoId` identifica a reserva relacionada à devolução dos ingressos e também participa do controle de deduplicação do processamento.

O registro de processamento e o efeito de negócio devem ser realizados de forma consistente, evitando que uma mesma ocorrência seja aplicada repetidamente ao estoque.

O reconhecimento (`ack`) da mensagem deve ocorrer somente após o processamento correspondente ter sido concluído com sucesso.

A estratégia de retenção dos registros utilizados para deduplicação deve ser definida separadamente, considerando que a manutenção indefinida desses registros pode provocar crescimento contínuo da estrutura de controle de eventos processados.
