# Entrega — Aula 02

## O que foi feito nesta etapa

- Domínio escolhido e registrado em ADR: venda de ingressos para eventos,
  com reserva sujeita a limite por CPF, pagamento externo simulado e
  compensação por expiração/recusa.
- Base do publisher (`servico-vendas`): evento `IngressoReservadoEvent`
  (particípio, imutável, `eventoId` próprio, data em ISO-8601) e
  `ItemDoIngressoVO`, mais `VendaConfig` (KafkaTemplate).
- `<preencher: VendaService — regra de negócio, chave de partição, envelope CloudEvents>`
- `<preencher: VendaCallbackService — tratamento do retorno do send()>`
- `<preencher: VendaController — endpoint HTTP, resposta 202>`
- `<preencher: consumer servico-ingressos aplicando o efeito e a compensação>`
- `<preencher: teste de idempotência — mesmo evento entregue 3x, efeito único>`

## Onde está cada coisa

- Domínio e critérios atendidos: [docs/adr/ADR-002-dominio-do-projeto.md](../adr/ADR-002-dominio-do-projeto.md)
- Padrões de pacote/nomenclatura: [AGENTS.md](../../AGENTS.md)
- Publisher: [servico-vendas](../../servico-vendas)
  - Evento: [IngressoReservadoEvent.java](../../servico-vendas/src/main/java/br/pucminas/aed/vendas/domain/IngressoReservadoEvent.java)
  - `<preencher: VendaService.java>` · `<preencher: VendaCallbackService.java>` · `<preencher: VendaController.java>`
- Consumer: [servico-ingressos](../../servico-ingressos)
- Registro de uso de IA: [docs/IA.md](../IA.md)
- `<preencher: link direto para o teste de idempotência, ex. servico-ingressos/src/test/...>`

## Por onde começar a leitura

1. ADR-002, para entender por que este domínio e como ele atende os quatro critérios.
2. [IngressoReservadoEvent.java](../../servico-vendas/src/main/java/br/pucminas/aed/vendas/domain/IngressoReservadoEvent.java)
   em `servico-vendas`, para ver o fato modelado (imutável, sem `record`, `eventoId` próprio).
3. `<preencher: VendaService.java>`, para ver a regra de decisão, o envelope CloudEvents e a chave de partição.
4. `<preencher: nome do listener/service>` em `servico-ingressos`, para ver a idempotência
   (dedup e efeito no mesmo commit, ack depois do commit).
5. O teste que entrega o mesmo evento três vezes.

## Como rodar

Ver [README.md](../../README.md#como-rodar-máquina-limpa) — resumo:

```powershell
docker compose up -d
cd servico-ingressos; ./mvnw.cmd spring-boot:run
cd servico-vendas; ./mvnw.cmd spring-boot:run
```

## Quem fez o quê

| Integrante                         | O que fez |
|------------------------------------|---|
| Gabriel Campos Ferreira Lisboa     | ADR-002, estrutura inicial do repositório, `docker-compose.yml`, base do publisher (`servico-vendas`: evento, VO, config) |
| Amir Gabriel Dantas Santos Andrade | Implementação da persistência no `servico-ingressos`: configuração do banco H2 em arquivo, criação automática das tabelas via `schema.sql`, implementação do `IngressoJdbcRepository` (`JdbcTemplate`) para controle de estoque por setor e deduplicação de eventos, e suite de testes de integração (`IngressoRepositoryTest`). |
| `<nome>`                           | `<preencher>` |
| `<nome>`                           | `<preencher>` |
| `<nome>`                           | `<preencher>` |
| `<nome>`                           | `<preencher>` |
| `<nome>`                           | `<preencher>` |
