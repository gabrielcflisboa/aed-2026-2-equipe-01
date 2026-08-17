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
2. Em um terminal, suba o consumidor na porta `8082`:
   ```powershell
   cd servico-ingressos
   ./mvnw.cmd spring-boot:run
   ```
3. Em outro terminal, suba o publisher:
   ```powershell
   cd servico-vendas
   ./mvnw.cmd spring-boot:run
   ```
4. Dispare uma reserva de ingresso:
   ```powershell
   $reserva = @{ compraId = "compra-demo-001"; cpfComprador = "000.000.000-00"; evento = "show-demo"; itens = @(@{ setor = "PISTA"; quantidade = 2; precoUnitario = 180.00 }) } | ConvertTo-Json -Depth 4
   Invoke-RestMethod http://localhost:8080/vendas/reservas -Method Post -ContentType "application/json" -Body $reserva
   ```
5. Para simular a recusa de pagamento e devolver o estoque:
   ```powershell
   Invoke-RestMethod http://localhost:8080/vendas/reservas/compra-demo-001/compensacoes -Method Post
   ```
6. Acompanhe os dois tópicos e os cabeçalhos `ce_*` no Kafka UI: http://localhost:8081

O consumer grava H2 em `./data/ingressos`. O teste
`IngressoListenerIdempotenciaTest` entrega a mesma reserva e a mesma compensação
três vezes e verifica que cada uma altera o estoque somente uma vez.

## Como testar manualmente

Com Kafka, consumer e publisher já em execução nos três terminais acima, execute
em um quarto PowerShell:

```powershell
$compraId = "compra-manual-001"
$reserva = @{
   compraId = $compraId
   cpfComprador = "000.000.000-00"
   evento = "show-manual"
   itens = @(
      @{ setor = "PISTA"; quantidade = 2; precoUnitario = 180.00 }
   )
} | ConvertTo-Json -Depth 4

Invoke-RestMethod http://localhost:8080/vendas/reservas `
   -Method Post `
   -ContentType "application/json" `
   -Body $reserva
```

O publisher responde `202 Accepted` com `eventoId` e `compraId`. No terminal do
consumer devem aparecer `Recebendo evento de ingresso reservado` e `Evento de
ingresso reservado processado`.

Simule a recusa do pagamento e confirme a compensação:

```powershell
Invoke-RestMethod "http://localhost:8080/vendas/reservas/$compraId/compensacoes" `
   -Method Post
```

O consumer deve registrar o recebimento e o processamento da compensação. No
Kafka UI, abra `vendas.ingresso.reservado.v1` e confira os headers
`ce_specversion`, `ce_id`, `ce_source`, `ce_type` e `ce_time`; o corpo deve
exibir `reservadoEm` em ISO-8601. A prova da redelivery ocorre com:

```powershell
cd servico-ingressos
.\mvnw.cmd test
```

O teste `IngressoListenerIdempotenciaTest` entrega a mesma reserva e a mesma
compensação três vezes e verifica um único débito e uma única devolução.

Para derrubar tudo (inclusive volumes):

```powershell
docker compose down -v
```
