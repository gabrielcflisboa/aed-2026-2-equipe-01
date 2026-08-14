# Equipe 01 · AED 2026/2

## Integrantes

| Nome completo                          | Matrícula     |
|----------------------------------------|---------------|
| Gabriel Campos Ferreira Lisboa (líder) | `255696`      |
| Maria Luísa Lacerda                    | `1418550`     |
| Amir Gabriel Dantas Santos Andrade     | `1666035`     |
| Pedro Assis Corrêa                     | `1265542`     |
| `<nome completo>`                      | `<matrícula>` |
| `<nome completo>`                      | `<matrícula>` |
| `<nome completo>`                      | `<matrícula>` |

## Domínio

Venda de ingressos para eventos: reserva sujeita a limite por CPF e
disponibilidade do setor, pagamento externo simulado, e compensação
(liberação do ingresso) quando a reserva expira ou o pagamento é recusado.
Detalhes e critérios atendidos em
[docs/adr/ADR-002-dominio-do-projeto.md](docs/adr/ADR-002-dominio-do-projeto.md).

## Estrutura

- `servico-vendas` — publisher: recebe a solicitação de reserva e publica `IngressoReservadoEvent`.
- `servico-ingressos` — consumer idempotente: aplica o efeito no estoque por setor e trata a compensação.

Padrões de pacote, nomenclatura e idempotência em [AGENTS.md](AGENTS.md).

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

Para derrubar tudo (inclusive volumes):
```powershell
docker compose down -v
```
