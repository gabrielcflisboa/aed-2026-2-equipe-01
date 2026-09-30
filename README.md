# Equipe 01 · AED 2026/2

## Integrantes

| Nome completo                          | Matrícula |
| -------------------------------------- | --------- |
| Gabriel Campos Ferreira Lisboa (líder) | `255696`  |
| Maria Luísa Lacerda                    | `257115`  |
| Amir Gabriel Dantas Santos Andrade     | `1666035` |
| Pedro Assis Corrêa                     | `256357`  |
| Thiago Felipe dos Santos               | `258087`  |
| Willian dos Santos Miranda             | `258173`  |

---

## Domínio

O sistema trata da venda de ingressos para eventos, como shows, jogos e peças.

Uma reserva está sujeita a regras de negócio como limite de ingressos por CPF e disponibilidade do setor. Depois que a reserva é aceita e o estoque é retirado, o pagamento é processado por um gateway externo simulado.

Quando o pagamento é recusado, o sistema executa uma Saga coreografada para liberar a reserva e devolver ao estoque exatamente os ingressos que aquela reserva havia retirado.

O estoque é mantido com event sourcing. O estado atual de um setor é reconstruído a partir de um log append-only, preservando o histórico de abertura, retirada e devolução de ingressos.

Detalhes do domínio estão registrados em:

- [ADR-002 — domínio do projeto](docs/adr/ADR-002-dominio-do-projeto.md)

---

## Estrutura

### `servico-vendas`

O `servico-vendas`:

- recebe solicitações de reserva via HTTP;
- valida limite de ingressos por CPF e regras da venda;
- publica `IngressoReservadoEvent`;
- mantém reservas, cotas por CPF e compras liberadas em memória;
- hospeda o gateway de pagamento simulado;
- publica `PagamentoRecusadoEvent` quando o pagamento é recusado;
- consome `PagamentoRecusadoEvent`;
- localiza a reserva correspondente;
- devolve a cota do CPF;
- publica `IngressoLiberadoEvent`;
- possui política de retentativa e DLQ para falhas no processamento da recusa.

O serviço não possui banco de dados próprio.

### `servico-ingressos`

O `servico-ingressos`:

- consome `IngressoReservadoEvent`;
- mantém o estoque por setor usando event sourcing;
- grava o efeito da reserva como `IngressoRetiradoEvent`;
- consome `IngressoLiberadoEvent`;
- usa `reservaEventoId` para localizar no histórico o que aquela reserva realmente retirou;
- grava a devolução como `IngressoDevolvidoEvent`;
- trata mensagens duplicadas de forma idempotente;
- mantém DLQs para reservas e liberações;
- oferece reprocessamento manual por `ce_id`;
- oferece consulta HTTP do estado e histórico do estoque;
- mantém também o agregador de reservas por setor/evento.

O event store é persistido no H2.

---

## Documentação

- [ADR-002 — domínio do projeto](docs/adr/ADR-002-dominio-do-projeto.md)
- [ADR-003 — chave de partição](docs/adr/ADR-003-chave-de-particao.md)
- [ADR-004 — contrato, agregador e compensação](docs/adr/ADR-004-contrato-agregador-e-compensacao.md)
- [ADR-005 — event sourcing no estoque](docs/adr/ADR-005-event-sourcing.md)
- [ADR-006 — resiliência, caminho de falha e Saga](docs/adr/ADR-006-resiliencia.md)
- [ADR-007 — projeções e replay](docs/adr/ADR-007-projecoes-e-replay.md)
- [Arquitetura do sistema](docs/arquitetura.md)
- [Contrato dos eventos](docs/contrato.md)
- [Apresentação](docs/apresentacao.pdf)
- [Uso de IA](docs/IA.md)
- [Entrega da aula 03](docs/entregas/aula-03.md)
- [Entrega da aula 04](docs/entregas/aula-04.md)
- [Entrega da aula 05](docs/entregas/aula-05.md)
- [Padrões obrigatórios do repositório](AGENTS.md)

---

## Topologia principal

A Saga do pagamento recusado usa três eventos:

| Evento | Tópico | Chave |
| --- | --- | --- |
| `IngressoReservadoEvent` | `vendas.ingresso.reservado.v1` | `evento` |
| `PagamentoRecusadoEvent` | `pagamentos.pagamento.recusado.v1` | `compraId` |
| `IngressoLiberadoEvent` | `vendas.ingresso.liberado.v1` | `evento` |

O fluxo é:

```text
POST /vendas/reservas
        |
        v
IngressoReservadoEvent
        |
        v
servico-ingressos
        |
        v
IngressoRetiradoEvent

POST /pagamentos/{compraId}/recusas
        |
        v
PagamentoRecusadoEvent
        |
        v
servico-vendas
        |
        v
IngressoLiberadoEvent
        |
        v
servico-ingressos
        |
        v
IngressoDevolvidoEvent