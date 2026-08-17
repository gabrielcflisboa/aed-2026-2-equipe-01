# Entrega — Aula 02

## O que foi feito nesta etapa

- Domínio escolhido e registrado em ADR: venda de ingressos para eventos,
  com reserva sujeita a limite por CPF, pagamento externo simulado e
  compensação por expiração/recusa.
- Base do publisher (`servico-vendas`): evento `IngressoReservadoEvent`
  (particípio, imutável, `eventoId` próprio, data em ISO-8601) e
  `ItemDoIngressoVO`, mais `VendaConfig` (KafkaTemplate).
- `VendaService`: regra de limite por CPF e capacidade, chave de partição por
  evento e publicação dos fatos de reserva e compensação.
- `VendaCallbackService`: envelope CloudEvents 1.0 binário e tratamento
  assíncrono do retorno de `send()`.
- `VendaController`: criado o endpoint `POST /vendas/reservas`, responsável por
  receber a solicitação de reserva, delegar o processamento ao `VendaService` e
  retornar `202 Accepted`, indicando que o efeito da reserva será processado de
  forma assíncrona.
- `servico-ingressos`: criada a representação própria de
  `IngressoReservadoEvent` no consumidor, independente da classe existente no
  publisher, com menos campos e tolerância a campos desconhecidos na
  desserialização.
- Consumer idempotente: débito/devolução do estoque e deduplicação por
  `eventoId` no mesmo commit; confirmação de offset depois do commit.
- Teste de idempotência: entrega cada evento três vezes e verifica efeito único.

## Onde está cada coisa

- Domínio e critérios atendidos: [docs/adr/ADR-002-dominio-do-projeto.md](../adr/ADR-002-dominio-do-projeto.md)
- Padrões de pacote/nomenclatura: [AGENTS.md](../../AGENTS.md)
- Publisher: [servico-vendas](../../servico-vendas)
  - Evento: [IngressoReservadoEvent.java](../../servico-vendas/src/main/java/br/pucminas/aed/vendas/domain/IngressoReservadoEvent.java)
  - [VendaService.java](../../servico-vendas/src/main/java/br/pucminas/aed/vendas/service/VendaService.java) · [VendaCallbackService.java](../../servico-vendas/src/main/java/br/pucminas/aed/vendas/service/VendaCallbackService.java) · [VendaController.java](../../servico-vendas/src/main/java/br/pucminas/aed/vendas/controller/VendaController.java)
- Consumer: [servico-ingressos](../../servico-ingressos)
  - [IngressoListener.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/controller/IngressoListener.java) · [IngressoService.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/IngressoService.java)
- Registro de uso de IA: [docs/IA.md](../IA.md)
- [Teste de idempotência](../../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/IngressoListenerIdempotenciaTest.java)

## Por onde começar a leitura

1. ADR-002, para entender por que este domínio e como ele atende os quatro critérios.
2. [IngressoReservadoEvent.java](../../servico-vendas/src/main/java/br/pucminas/aed/vendas/domain/IngressoReservadoEvent.java)
   em `servico-vendas`, para ver o fato modelado (imutável, sem `record`, `eventoId` próprio).
3. `VendaService.java`, para ver a regra de decisão, o envelope CloudEvents e a chave de partição.
4. `IngressoListener` e `IngressoService` em `servico-ingressos`, para ver a idempotência
   (dedup e efeito no mesmo commit, ack depois do commit).
5. `IngressoListenerIdempotenciaTest`, que entrega o mesmo evento três vezes.

## Como rodar

Ver [README.md](../../README.md#como-rodar-máquina-limpa) e
[Como testar manualmente](../../README.md#como-testar-manualmente). Resumo:

```powershell
docker compose up -d
cd servico-ingressos; ./mvnw.cmd spring-boot:run
cd servico-vendas; ./mvnw.cmd spring-boot:run
```

## Quem fez o quê

| Integrante                         | O que fez |
|------------------------------------|---|
| Gabriel Campos Ferreira Lisboa     | ADR-002, estrutura inicial do repositório, `docker-compose.yml`, base do publisher (`servico-vendas`: evento, VO, config); correções pendentes do fluxo completo: publicação CloudEvents, compensação, consumer idempotente, teste de entrega tripla, configuração de portas/Compose e documentação de execução e teste manual. |
| Amir Gabriel Dantas Santos Andrade | Implementação da persistência no `servico-ingressos`: configuração do banco H2 em arquivo, criação automática das tabelas via `schema.sql`, implementação do `IngressoJdbcRepository` (`JdbcTemplate`) para controle de estoque por setor e deduplicação de eventos, e suite de testes de integração (`IngressoRepositoryTest`). |
| Maria Luísa Lacerda | `VendaController`, endpoint `POST /vendas/reservas` com resposta `202 Accepted` e representação tolerante de `IngressoReservadoEvent` no consumer. |
| Pedro Assis Corrêa | Envelope CloudEvents e tratamento assíncrono do retorno de `send()`. |
| Thiago Felipe dos Santos | Contribuições registradas no histórico Git da equipe. |
| Willian dos Santos Miranda | Contribuições registradas no histórico Git da equipe. |