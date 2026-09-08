# Equipe 01 · AED 2026/2

## Integrantes

| Nome completo                          | Matrícula     |
| -------------------------------------- | ------------- |
| Gabriel Campos Ferreira Lisboa (líder) | `255696`      |
| Maria Luísa Lacerda                    | `1418550`     |
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

Documentos:

- [ADR-002 — domínio do projeto](docs/adr/ADR-002-dominio-do-projeto.md)
- [ADR-005 — event sourcing no estoque](docs/adr/ADR-005-event-sourcing.md)
- [Contrato do evento `IngressoReservadoEvent`](docs/contrato.md)
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

## Testes

```powershell
cd servico-vendas;    ./mvnw.cmd test   # 5 testes
cd servico-ingressos; ./mvnw.cmd test   # 15 testes
```
