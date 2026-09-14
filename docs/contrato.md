# Contrato do Evento — Ingresso Reservado

## 1. Identificação do evento

**Tipo do evento:** `vendas.ingresso.reservado.v1`

O evento `vendas.ingresso.reservado.v1` representa o fato de que uma reserva de ingressos foi aceita para um comprador. O evento é publicado pelo serviço `servico-vendas` no tópico de reservas e pode ser consumido por diferentes consumidores de forma independente.

O evento representa um fato ocorrido no domínio, e não um comando. O campo `eventoId` identifica unicamente a ocorrência do evento. Os consumidores podem utilizá-lo como identificador para controle de idempotência e deduplicação.

---

## 2. Estrutura da carga

A carga (`data`) do evento é serializada em JSON. O envelope CloudEvents 1.0 é transportado em modo binário, com seus atributos representados nos headers da mensagem Kafka.

O contrato descrito neste documento refere-se à carga (`data`) do evento `IngressoReservadoEvent`; os atributos do envelope CloudEvents são tratados separadamente.

| Característica        | Definição                             |
| --------------------- | ------------------------------------- |
| Formato da carga      | JSON                                  |
| Formato do envelope   | CloudEvents 1.0 — Binary Content Mode |
| Transporte            | Apache Kafka                          |
| Atributos CloudEvents | Headers `ce_*`                        |

A carga possui os seguintes campos:

| Campo          | Tipo                          | Obrigatório | Significado                                                                                          |
| -------------- | ----------------------------- | ----------- | ---------------------------------------------------------------------------------------------------- |
| `eventoId`     | `string`                      | Sim         | Identifica unicamente a ocorrência deste evento e permite aos consumidores controlar duplicidades.   |
| `compraId`     | `string`                      | Sim         | Identifica a compra à qual a reserva de ingressos está associada.                                    |
| `cpfComprador` | `string`                      | Sim         | Identifica o comprador utilizado para aplicar a regra de limite de ingressos por CPF.                |
| `evento`       | `string`                      | Sim         | Identifica o evento de entretenimento para o qual os ingressos foram reservados.                     |
| `itens`        | `array<ItemDoIngressoVO>`     | Sim         | Representa os itens de ingresso que compõem a reserva, incluindo setor, quantidade e preço unitário. |
| `reservadoEm`  | `string (ISO-8601 date-time)` | Sim         | Representa o instante em que a reserva foi aceita no domínio.                                        |

### 2.1 Estrutura de `ItemDoIngressoVO`

Cada elemento da lista `itens` possui os seguintes campos:

| Campo           | Tipo      | Obrigatório | Significado                                                                         |
| --------------- | --------- | ----------- | ----------------------------------------------------------------------------------- |
| `setor`         | `string`  | Sim         | Identifica o setor do evento no qual os ingressos foram reservados.                 |
| `quantidade`    | `integer` | Sim         | Informa a quantidade de ingressos reservados para o setor. Deve ser maior que zero. |
| `precoUnitario` | `number`  | Sim         | Representa o preço de um ingresso reservado naquele setor.                          |

Os campos de `IngressoReservadoEvent` são obrigatórios no construtor e não podem receber `null`. A lista `itens` também é copiada de forma defensiva, preservando a imutabilidade do evento.

Em `ItemDoIngressoVO`, `setor` e `precoUnitario` não podem ser nulos e `quantidade` deve ser maior que zero.

---

## 3. Formato das datas

O campo `reservadoEm` utiliza o tipo `Instant` na implementação e é serializado como texto.

O formato adotado pelo contrato é **ISO-8601**, nunca epoch.

Exemplo:

```text
2026-08-18T20:30:00Z
```

O campo representa o momento em que a reserva foi aceita no domínio, e não o momento em que a mensagem foi recebida pelo consumidor.

---

## 4. Chave de partição

A chave de partição utilizada na publicação do evento é:

```text
evento
```

A utilização de `evento` como chave faz com que eventos referentes ao mesmo evento de entretenimento sejam direcionados à mesma partição Kafka.

Dessa forma, a ordem dos eventos é garantida dentro da partição para uma mesma chave `evento`. Essa ordenação é importante porque as reservas de um mesmo evento disputam a disponibilidade de ingressos por setor.

A chave de partição não garante uma ordem global entre eventos pertencentes a chaves diferentes.

---

## 5. Envelope CloudEvents

O evento utiliza o envelope **CloudEvents 1.0** em **Binary Content Mode**. Nesse modo, os atributos do envelope são transportados nos headers da mensagem Kafka, enquanto a carga do evento permanece no valor da mensagem.

Os atributos utilizados são:

* `ce_specversion`: versão da especificação CloudEvents;
* `ce_id`: identificador da ocorrência do evento;
* `ce_source`: identifica a origem responsável pela publicação do evento;
* `ce_type`: identifica o tipo do evento, correspondente a `vendas.ingresso.reservado.v1`;
* `ce_time`: momento da ocorrência do fato no domínio.

O atributo `ce_time` representa o horário de ocorrência da reserva e não o horário de recebimento da mensagem pelo broker ou consumidor.

---

## 6. Regra de compatibilidade

A regra de compatibilidade escolhida é **BACKWARD**.

A escolha permite que uma nova versão do consumidor leia eventos produzidos pela versão anterior do produtor. Dessa forma, a equipe pode implantar primeiro o consumidor e, posteriormente, atualizar o produtor, reduzindo a necessidade de uma implantação coordenada ou de uma janela de manutenção.

Essa estratégia também é coerente com a característica de tolerância a campos desconhecidos adotada pelo consumidor do projeto.

---

## 7. Evolução do contrato

Alterações no significado dos campos existentes devem ser tratadas como mudanças de contrato, mesmo quando o tipo do campo permanece o mesmo.

Por exemplo, alterar o significado de `precoUnitario` para representar o preço total do item manteria o tipo numérico, mas mudaria o significado consumido pelos demais serviços. Essa alteração não deve ser considerada apenas uma mudança de implementação, pois pode produzir resultados incorretos nos consumidores sem necessariamente gerar um erro de processamento.

---

## 8. Exemplo de carga

O exemplo abaixo utiliza dados fictícios:

```json
{
  "eventoId": "evt-001",
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

Todos os valores apresentados no exemplo são fictícios e servem apenas para demonstrar o contrato.
