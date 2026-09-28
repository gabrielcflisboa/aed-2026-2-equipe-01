# ADR-006: resiliência, caminho de falha e a Saga do pagamento recusado

## Status

Proposta · 2026-09-25 · Equipe 01

Autoria: Pedro Assis Corrêa (256357) nas seções 1 a 4, 7 e 8; Maria Luísa Lacerda (257115) nas seções 5 e 6.

Relaciona-se com o ADR-002, o ADR-003, o ADR-005 e o ADR-007.

## Contexto

<!-- Pessoa A: tratador padrão do Spring Kafka e a plataforma Spring Boot 4.1. -->
<!-- Pessoa B: como a compensação funcionava antes e os defeitos dela. -->

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

Coreografia: o gateway simulado publica `PagamentoRecusadoEvent`, o servico-vendas publica `IngressoLiberadoEvent` e o servico-ingressos devolve ao setor o que a reserva retirou.

### 6. Quando a própria compensação falha

A liberação retenta como qualquer evento, vai para a DLQ quando esgota e é idempotente pela reserva que desfaz.

### 7. Retenção da deduplicação

45 dias, mais que a retenção da DLQ.

### 8. Plataforma

Registro retroativo do uso de Spring Boot 4.1, Spring Kafka 4.1 e Jackson 3.

## Alternativas consideradas

<!-- Cada pessoa acrescenta as suas. -->

## Consequências aceitas

<!-- Cada pessoa acrescenta as suas. -->