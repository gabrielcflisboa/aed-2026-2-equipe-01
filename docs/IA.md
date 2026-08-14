# Registro do uso de IA

## Aula 02

### Pedro Assis Corrêa (256357) — tarefa 2: envelope CloudEvents e retorno do `send()`

Ferramenta: Claude (Claude Code).
Arquivos afetados: [`VendaCallbackService.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/service/VendaCallbackService.java),
[`VendaCallbackServiceTest.java`](../servico-vendas/src/test/java/br/pucminas/aed/vendas/service/VendaCallbackServiceTest.java),
[`application.yml`](../servico-vendas/src/main/resources/application.yml).

---

#### Interação 1 — como preencher `ce_time`

**Pedido:** como montar os cinco cabeçalhos `ce_*` do CloudEvents 1.0 em modo binário no `ProducerRecord`.

**Sugerido:** montar os headers com `RecordHeader`/`headers().add(...)` em UTF-8, e preencher
`ce_time` com `Instant.now()` no momento do envio — que é a leitura literal de "timestamp ISO-8601".

**Aceito:** a montagem dos headers no `ProducerRecord`, com os valores em UTF-8.

**RECUSADO:** `ce_time = Instant.now()`. A especificação CloudEvents define `time` como o instante em
que **a ocorrência aconteceu**, não o instante do transporte. Como o produtor Kafka pode reenviar a
mensagem internamente (`retries`), `now()` faria o cabeçalho divergir do campo `reservadoEm` do corpo
— duas verdades para o mesmo fato, e o consumidor não teria como saber qual vale. Usamos
`evento.getReservadoEm()`, que também torna o valor determinístico e testável.

Efeito na saída real (consumida do tópico): `ce_time:2026-08-14T12:10:43.518380Z` e
`"reservadoEm":"2026-08-14T12:10:43.518380Z"` — o mesmo instante nos dois lugares.

---

#### Interação 2 — como saber se a publicação deu certo

**Pedido:** como tratar o retorno de `kafkaTemplate.send(...)` para não engolir falha de publicação.

**Sugerido:** duas alternativas apareceram — chamar `.get()` no retorno para ler o `SendResult`
de forma síncrona, ou usar `ListenableFuture.addCallback(...)`.

**Aceito:** nenhuma das duas, na forma sugerida.

**RECUSADO (1):** `.get()`/`.join()` no retorno. Bloquearia a thread do request HTTP esperando o
broker confirmar, o que contradiz o `202 Accepted` da tarefa 3: o 202 existe justamente porque o
efeito **ainda não aconteceu** no instante da resposta. Sob indisponibilidade do broker, o request
ficaria pendurado até o `delivery.timeout.ms`.

**RECUSADO (2):** `ListenableFuture.addCallback(...)`. API removida no Spring Kafka 3+ — o projeto
está no Spring Boot 4.1, onde `send()` devolve `CompletableFuture`. Sugestão baseada em material
desatualizado; não compilaria.

**Adotado:** `whenComplete((resultado, falha) -> ...)`, assíncrono. Falha vira `log.error`, sucesso
loga partição e offset. A falha é registrada e **não** propagada: quem chamou já respondeu 202, e
exceção lançada dentro de callback assíncrona não chegaria ao cliente HTTP de qualquer forma.

---
