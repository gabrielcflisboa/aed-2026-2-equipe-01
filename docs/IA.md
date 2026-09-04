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

## Aula 05

### Pedro Assis Corrêa (256357): event sourcing do estoque e projeções

Ferramenta: Claude (Claude Code).
Arquivos afetados: todo o `servico-ingressos`, com agregado, event store,
projeções e testes; [`ADR-005`](adr/ADR-005-event-sourcing.md);
[`aula-05.md`](entregas/aula-05.md); e correções de bloqueadores em
[`VendaService.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/service/VendaService.java)
e [`VendaConfig.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/VendaConfig.java).

---

#### Interação 1: qual é o agregado

**Pedido:** qual entidade do domínio de venda de ingressos deveria virar o
agregado com event sourcing, dado o ADR-002.

**Sugerido:** três candidatos, com o trade-off de cada um. `EstoqueDoSetor` com
stream por `(evento, setor)`; `Reserva`, um stream por compra; e o evento de
entretenimento inteiro como um único stream.

**Aceito:** `EstoqueDoSetor`, stream `(evento, setor)`. O argumento que decidiu
foi o da fronteira de consistência. A única invariante do domínio, não vender
mais do que a capacidade, se resolve inteiramente dentro de um setor de um
evento, e é ali que dois compradores disputam o mesmo assento, que é a razão pela
qual o ADR-002 escolheu este domínio.

**RECUSADO (1):** `Reserva` como agregado do estoque. Uma reserva isolada não
sabe se cabe. A checagem de capacidade voltaria a depender de uma leitura
externa, quase certamente de uma projeção, que é o que a aula alerta contra. Ela
provavelmente volta na Saga, para o ciclo de vida do pagamento, mas não como dona
do estoque.

**RECUSADO (2):** o evento inteiro como um stream só. Toda venda do show
competiria pela mesma versão. Ganharíamos uma invariante que o domínio não pede,
a capacidade total do evento, ao custo de serializar vendas que não disputam nada
entre si.

---

#### Interação 2: como o estoque inicial entra no sistema

**Pedido:** como popular a capacidade dos setores, já que a tabela
`estoque_setor` da aula 02 nunca era semeada.

**Sugerido:** um `data.sql` com `INSERT INTO` para cada setor, que é a resposta
correta para uma tabela mutável e foi inclusive a correção apontada na revisão da
aula 02.

**RECUSADO.** Com event sourcing essa resposta se inverte. Capacidade inserida
direto na tabela é estado que o replay não reproduz: a primeira reconstrução da
projeção apagaria as linhas do `data.sql` e o estoque inteiro iria a zero. Pior,
o erro só apareceria na hora de demonstrar o critério da aula.

**Adotado:** a abertura do setor virou o primeiro fato do stream
(`SetorAbertoEvent`), gravado pelo `AberturaDeSetoresService` no arranque, uma vez
por stream. A capacidade passa a sobreviver a qualquer replay porque ela *é*
parte do log.

---

#### Interação 3: de onde a decisão lê o estoque

**Pedido:** o `IngressoService` relê o stream inteiro a cada mensagem para saber
quanto resta. Dava para ler o número pronto da projeção
`disponibilidade_por_setor`?

**Sugerido:** sim. A projeção já tem a coluna `disponivel` calculada, e a leitura
seria uma linha só em vez de um replay.

**RECUSADO.** É a armadilha exata que o enunciado descreve. No momento em que a
decisão de aceitar ou recusar passa a depender da projeção, a projeção vira fonte
da verdade sem ninguém ter decidido isso, e o atraso do catch-up, que era um
inconveniente visual, passa a vender o mesmo assento duas vezes. A projeção
continua sendo só leitura de tela. Quem decide é o agregado, reconstruído do
stream.

Essa recusa é o que sustenta a tabela de defasagem por tela da folha de entrega.
As tolerâncias podem ser generosas *porque* nenhuma decisão depende delas.

---

#### Interação 4: compensação apagando o passado

**Pedido:** como devolver ingressos ao estoque quando a reserva expira ou o
pagamento é recusado.

**Sugerido, entre outras opções:** remover do log o `IngressoRetiradoEvent`
correspondente, já que o efeito precisa ser desfeito.

**RECUSADO.** Log append-only não tem `DELETE`. Apagar o fato faria a reserva
desaparecer da história, e depois do replay ninguém saberia que ela existiu, que
é justamente a auditoria que o ADR-002 listou como algo que vale reprocessar.

**Adotado:** `IngressoDevolvidoEvent` como fato novo, que anula o *efeito* do
anterior sem apagá-lo. Os dois continuam no log depois de qualquer reconstrução.
O `EventoDoEstoqueRepository` foi escrito sem nenhum método de atualizar ou
remover, para que a regra não dependa de disciplina de quem escreve o código.

---

#### Interação 5: duas sugestões que o teste derrubou

Registro pelo valor de método. As duas foram aceitas, entraram no código e só
caíram quando a suíte rodou.

**`INSERT ... SELECT ? WHERE NOT EXISTS (...)`** para criar a linha da projeção de
ocupação apenas se ela não existisse. Compila e parece correto, mas no H2 o
`SELECT` sem `FROM` não filtra pelo `WHERE`. O insert acontecia sempre e estourava
violação de chave primária. Substituído por
`MERGE INTO ocupacao_por_evento (evento) KEY (evento)` com `DEFAULT 0` nas
colunas de contador, porque o MERGE do H2 só atualiza as colunas citadas e a
linha existente não é zerada.

**`abertura: {}`** no `application.yml` de teste, para dizer "mapa vazio". O
binder do Spring lê isso como a *string* `"{}"` e falha com
`ConverterNotFoundException`. A correção foi omitir a chave, porque o campo já
nasce como `LinkedHashMap` vazio.

---
