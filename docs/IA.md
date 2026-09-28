# Registro do uso de IA

## Aula 02

### Pedro Assis CorrÃªa (256357) â€” tarefa 2: envelope CloudEvents e retorno do `send()`

Ferramenta: Claude (Claude Code).
Arquivos afetados: [`VendaCallbackService.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/service/VendaCallbackService.java),
[`VendaCallbackServiceTest.java`](../servico-vendas/src/test/java/br/pucminas/aed/vendas/service/VendaCallbackServiceTest.java),
[`application.yml`](../servico-vendas/src/main/resources/application.yml).

---

#### InteraÃ§Ã£o 1 â€” como preencher `ce_time`

**Pedido:** como montar os cinco cabeÃ§alhos `ce_*` do CloudEvents 1.0 em modo binÃ¡rio no `ProducerRecord`.

**Sugerido:** montar os headers com `RecordHeader`/`headers().add(...)` em UTF-8, e preencher
`ce_time` com `Instant.now()` no momento do envio â€” que Ã© a leitura literal de "timestamp ISO-8601".

**Aceito:** a montagem dos headers no `ProducerRecord`, com os valores em UTF-8.

**RECUSADO:** `ce_time = Instant.now()`. A especificaÃ§Ã£o CloudEvents define `time` como o instante em
que **a ocorrÃªncia aconteceu**, nÃ£o o instante do transporte. Como o produtor Kafka pode reenviar a
mensagem internamente (`retries`), `now()` faria o cabeÃ§alho divergir do campo `reservadoEm` do corpo
â€” duas verdades para o mesmo fato, e o consumidor nÃ£o teria como saber qual vale. Usamos
`evento.getReservadoEm()`, que tambÃ©m torna o valor determinÃ­stico e testÃ¡vel.

Efeito na saÃ­da real (consumida do tÃ³pico): `ce_time:2026-08-14T12:10:43.518380Z` e
`"reservadoEm":"2026-08-14T12:10:43.518380Z"` â€” o mesmo instante nos dois lugares.

---

#### InteraÃ§Ã£o 2 â€” como saber se a publicaÃ§Ã£o deu certo

**Pedido:** como tratar o retorno de `kafkaTemplate.send(...)` para nÃ£o engolir falha de publicaÃ§Ã£o.

**Sugerido:** duas alternativas apareceram â€” chamar `.get()` no retorno para ler o `SendResult`
de forma sÃ­ncrona, ou usar `ListenableFuture.addCallback(...)`.

**Aceito:** nenhuma das duas, na forma sugerida.

**RECUSADO (1):** `.get()`/`.join()` no retorno. Bloquearia a thread do request HTTP esperando o
broker confirmar, o que contradiz o `202 Accepted` da tarefa 3: o 202 existe justamente porque o
efeito **ainda nÃ£o aconteceu** no instante da resposta. Sob indisponibilidade do broker, o request
ficaria pendurado atÃ© o `delivery.timeout.ms`.

**RECUSADO (2):** `ListenableFuture.addCallback(...)`. API removida no Spring Kafka 3+ â€” o projeto
estÃ¡ no Spring Boot 4.1, onde `send()` devolve `CompletableFuture`. SugestÃ£o baseada em material
desatualizado; nÃ£o compilaria.

**Adotado:** `whenComplete((resultado, falha) -> ...)`, assÃ­ncrono. Falha vira `log.error`, sucesso
loga partiÃ§Ã£o e offset. A falha Ã© registrada e **nÃ£o** propagada: quem chamou jÃ¡ respondeu 202, e
exceÃ§Ã£o lanÃ§ada dentro de callback assÃ­ncrona nÃ£o chegaria ao cliente HTTP de qualquer forma.

---

## Aula 03

### Gabriel Campos Ferreira Lisboa (255696) â€” agregador por janela de tempo

Ferramenta: GitHub Copilot.
Arquivos afetados: `AgregadorDeReservasListener.java`,
`AgregacaoDeReservasService.java`, `AgregacaoJdbcRepository.java`,
`IngressoReservadoEvent.java` (adiÃ§Ã£o de `reservadoEm`), `schema.sql`,
`AgregacaoDeReservasServiceTest.java`.

**Pedido:** implementar um segundo consumidor, com `group.id` prÃ³prio, que
agregasse o fluxo de reservas por janela de tempo, respondendo a uma
pergunta de negÃ³cio.

**Sugerido:** usar processing time (`Instant.now()` no momento em que o
listener recebe a mensagem) para calcular a janela, por ser mais simples de
implementar e nÃ£o depender de nenhum campo do payload.

**RECUSADO:** processing time para esta pergunta. A pergunta agregada Ã©
"quantos ingressos foram reservados por setor/evento" â€” um fato do domÃ­nio,
nÃ£o da infraestrutura de consumo. Com processing time, reprocessar o tÃ³pico
do inÃ­cio (por exemplo depois de corrigir um bug no agregador) mudaria o
resultado, jÃ¡ que cada mensagem cairia em uma janela diferente dependendo de
quando fosse lida. Isso tornaria a agregaÃ§Ã£o nÃ£o confiÃ¡vel como fonte de
relatÃ³rio.

**Adotado:** event time, lendo `reservadoEm` do prÃ³prio payload (campo que jÃ¡
existe no evento do publisher, mas que o consumidor original â€” por ser
tolerante e minimalista â€” nÃ£o declarava). O consumidor da aula 02 continua
ignorando esse e outros campos que nÃ£o usa; sÃ³ o novo evento consumido pelo
agregador passou a declarar `reservadoEm`. Isso torna o resultado da
agregaÃ§Ã£o determinÃ­stico sob reprocessamento, o que o teste
`AgregacaoDeReservasServiceTest` confirma diretamente ao gravar eventos fora
de ordem de chegada e verificar que cada um cai na janela correta.

---

### Amir Gabriel Dantas Santos Andrade (1666035) â€” compensaÃ§Ã£o por pagamento recusado

Ferramenta: Claude (Claude Code).
Arquivos afetados: [`GatewayDePagamentoService.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/service/GatewayDePagamentoService.java),
[`VendaCompensacaoCallbackService.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/service/VendaCompensacaoCallbackService.java),
[`VendaService.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/service/VendaService.java),
[`VendaController.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/controller/VendaController.java),
[`VendaConfig.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/VendaConfig.java),
[`IngressoService.java`](../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/IngressoService.java),
[`IngressoListener.java`](../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/controller/IngressoListener.java),
[`contrato.md`](contrato.md) e [`aula-03.md`](entregas/aula-03.md).

---

#### InteraÃ§Ã£o 1 â€” como publicar o evento de compensaÃ§Ã£o

**Pedido:** ligar o `IngressoReservaCompensadaEvent` ao Kafka, jÃ¡ que o `KafkaTemplate` existente era
`KafkaTemplate<String, IngressoReservadoEvent>`.

**Sugerido:** generalizar para um Ãºnico `KafkaTemplate<String, Object>`, com tÃ³pico, `ce_type`, id e
instante virando parÃ¢metros do `publicar(...)`, porque o Kafka sÃ³ transporta bytes e a tipagem existe
apenas no compilador Java.

**Aceito:** a explicaÃ§Ã£o de que a tipagem nÃ£o existe no Kafka, sÃ³ no lado do `servico-vendas`, e portanto
nÃ£o centraliza nada nem Ã© risco entre serviÃ§os.

**RECUSADO:** o `KafkaTemplate<String, Object>` genÃ©rico. A equipe preferiu o padrÃ£o que o projeto jÃ¡ usa
(um template tipado por evento): dÃ¡ seguranÃ§a em tempo de compilaÃ§Ã£o, sabemos exatamente qual evento cada
serviÃ§o de publicaÃ§Ã£o envia, e nÃ£o dÃ¡ para publicar o evento errado no tÃ³pico errado.

**Adotado:** segundo bean `kafkaTemplateCompensacao` no `VendaConfig` e classe irmÃ£
`VendaCompensacaoCallbackService`. O `VendaCallbackService` e o teste dele nÃ£o foram alterados.

---

#### InteraÃ§Ã£o 2 â€” quem decide a recusa do pagamento

**Pedido:** simular o pagamento recusado que dispara a compensaÃ§Ã£o.

**Sugerido:** endpoint `POST /vendas/reservas/{compraId}/compensacoes` chamando direto
`VendaService.compensarPagamentoRecusado(compraId)`, que jÃ¡ assumia que a recusa aconteceu.

**RECUSADO:** deixar a decisÃ£o de recusar dentro do `VendaService`. Um gateway de pagamento Ã© um sistema
externo; a lÃ³gica dele nÃ£o pode morar na regra de negÃ³cio do `servico-vendas`, que deve apenas reagir a uma
recusa ocorrida em outro lugar.

**Adotado:** `GatewayDePagamentoService` simula o gateway e devolve o motivo; o controller liga as duas
peÃ§as. O campo `motivo` passou a viajar no evento atÃ© o `IngressoDevolvidoEvent`, em vez de um texto fixo
no consumidor. O `reservasAceitas` (mapa em memÃ³ria) Ã© a simplificaÃ§Ã£o da simulaÃ§Ã£o: num cenÃ¡rio real seria
uma tabela de compras do prÃ³prio `servico-vendas`.

---

#### InteraÃ§Ã£o 3 â€” identidade do evento de compensaÃ§Ã£o e idempotÃªncia

**Pedido:** consumir a compensaÃ§Ã£o no `servico-ingressos` devolvendo o estoque.

**Sugerido:** ao ler o cÃ³digo, a IA apontou dois problemas: o `contrato.md` mandava reusar o `eventoId` da
reserva original no evento de compensaÃ§Ã£o, e o `IngressoService.compensar(...)` nÃ£o registrava deduplicaÃ§Ã£o
(registrar por item quebraria a partir do segundo item da mesma mensagem).

**RECUSADO:** reaproveitar o `eventoId` da reserva. A tabela `evento_processado` usa `eventoId` como chave e
Ã© compartilhada; a compensaÃ§Ã£o seria descartada como "jÃ¡ processada" e a devoluÃ§Ã£o sumiria sem erro.

**Adotado:** `eventoId` novo por compensaÃ§Ã£o, com o `contrato.md` corrigido (seÃ§Ãµes 9 a 12 e 14, incluindo
`compraId`, `evento`, `motivo` e `compensadoEm`). O `processarCompensacao(...)` registra a deduplicaÃ§Ã£o uma
vez por mensagem e reaproveita o `compensar(...)` por item. O teste entrega a mesma compensaÃ§Ã£o 3 vezes e
confere um Ãºnico `IngressoDevolvido`. Verificado ponta a ponta com Kafka real: reserva, recusa, e o estoque
voltou a 100.

---

## Aula 05

### Pedro Assis CorrÃªa (256357): event sourcing do estoque

Ferramenta: Claude (Claude Code).
Arquivos afetados: todo o `servico-ingressos`, com agregado, event store e
testes; [`ADR-005`](adr/ADR-005-event-sourcing.md);
[`aula-05.md`](entregas/aula-05.md); e correÃ§Ãµes de bloqueadores em
[`VendaService.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/service/VendaService.java)
e [`VendaConfig.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/VendaConfig.java).

---

#### InteraÃ§Ã£o 1: qual Ã© o agregado

**Pedido:** qual entidade do domÃ­nio de venda de ingressos deveria virar o
agregado com event sourcing, dado o ADR-002.

**Sugerido:** trÃªs candidatos, com o trade-off de cada um. `EstoqueDoSetor` com
stream por `(evento, setor)`; `Reserva`, um stream por compra; e o evento de
entretenimento inteiro como um Ãºnico stream.

**Aceito:** `EstoqueDoSetor`, stream `(evento, setor)`. O argumento que decidiu
foi o da fronteira de consistÃªncia. A Ãºnica invariante do domÃ­nio, nÃ£o vender
mais do que a capacidade, se resolve inteiramente dentro de um setor de um
evento, e Ã© ali que dois compradores disputam o mesmo assento, que Ã© a razÃ£o pela
qual o ADR-002 escolheu este domÃ­nio.

**RECUSADO (1):** `Reserva` como agregado do estoque. Uma reserva isolada nÃ£o
sabe se cabe. A checagem de capacidade voltaria a depender de uma leitura
externa, quase certamente de uma projeÃ§Ã£o, que Ã© o que a aula alerta contra. Ela
provavelmente volta na Saga, para o ciclo de vida do pagamento, mas nÃ£o como dona
do estoque.

**RECUSADO (2):** o evento inteiro como um stream sÃ³. Toda venda do show
competiria pela mesma versÃ£o. GanharÃ­amos uma invariante que o domÃ­nio nÃ£o pede,
a capacidade total do evento, ao custo de serializar vendas que nÃ£o disputam nada
entre si.

---

#### InteraÃ§Ã£o 2: como o estoque inicial entra no sistema

**Pedido:** como popular a capacidade dos setores, jÃ¡ que a tabela
`estoque_setor` da aula 02 nunca era semeada.

**Sugerido:** um `data.sql` com `INSERT INTO` para cada setor, que Ã© a resposta
correta para uma tabela mutÃ¡vel e foi inclusive a correÃ§Ã£o apontada na revisÃ£o da
aula 02.

**RECUSADO.** Com event sourcing essa resposta se inverte. Capacidade inserida
direto na tabela Ã© estado que o log nÃ£o conhece: o agregado se reconstrÃ³i sÃ³ a
partir dos eventos do stream, entÃ£o nasceria com capacidade zero e recusaria toda
reserva. O `data.sql` ficaria lÃ¡, correto e ignorado.

**Adotado:** a abertura do setor virou o primeiro fato do stream
(`SetorAbertoEvent`), gravado pelo `AberturaDeSetoresService` no arranque, uma vez
por stream. A capacidade passa a sobreviver a qualquer replay porque ela *Ã©*
parte do log.

---

#### InteraÃ§Ã£o 3: de onde a decisÃ£o lÃª o estoque

**Pedido:** o `IngressoService` relÃª o stream inteiro a cada mensagem para saber
quanto resta. Dava para manter uma tabela `disponibilidade_por_setor` com o saldo
jÃ¡ calculado e ler uma linha sÃ³?

**Sugerido:** sim, uma projeÃ§Ã£o derivada do log, com a coluna `disponivel` jÃ¡
pronta. A decisÃ£o viraria uma consulta de uma linha em vez de um replay.

**RECUSADO.** Ã‰ a armadilha exata que o enunciado descreve. No momento em que a
decisÃ£o de aceitar ou recusar depende de uma tabela derivada, essa tabela vira
fonte da verdade sem ninguÃ©m ter decidido isso, e o atraso da atualizaÃ§Ã£o, que
seria um inconveniente visual, passa a vender o mesmo assento duas vezes. Quem
decide Ã© o agregado, reconstruÃ­do do stream.

A recusa tambÃ©m define o lugar da primeira projeÃ§Ã£o quando ela entrar: tela, e
nada alÃ©m disso. Por isso esta entrega para no event store, e a folha de entrega
registra a Ãºnica leitura que existe hoje, com defasagem zero.

---

#### InteraÃ§Ã£o 4: compensaÃ§Ã£o apagando o passado

**Pedido:** como devolver ingressos ao estoque quando a reserva expira ou o
pagamento Ã© recusado.

**Sugerido, entre outras opÃ§Ãµes:** remover do log o `IngressoRetiradoEvent`
correspondente, jÃ¡ que o efeito precisa ser desfeito.

**RECUSADO.** Log append-only nÃ£o tem `DELETE`. Apagar o fato faria a reserva
desaparecer da histÃ³ria, e depois do replay ninguÃ©m saberia que ela existiu, que
Ã© justamente a auditoria que o ADR-002 listou como algo que vale reprocessar.

**Adotado:** `IngressoDevolvidoEvent` como fato novo, que anula o *efeito* do
anterior sem apagÃ¡-lo. Os dois continuam no log depois de qualquer reconstruÃ§Ã£o.
O `EventoDoEstoqueRepository` foi escrito sem nenhum mÃ©todo de atualizar ou
remover, para que a regra nÃ£o dependa de disciplina de quem escreve o cÃ³digo.

---

#### InteraÃ§Ã£o 5: a sugestÃ£o que o teste derrubou

Registro pelo valor de mÃ©todo. Foi aceita, entrou no cÃ³digo e sÃ³ caiu quando a
suÃ­te rodou.

**`abertura: {}`** no `application.yml` de teste, para dizer "mapa vazio". O
binder do Spring lÃª isso como a *string* `"{}"` e falha com
`ConverterNotFoundException`. A correÃ§Ã£o foi omitir a chave, porque o campo jÃ¡
nasce como `LinkedHashMap` vazio.

---

### Amir Gabriel Dantas Santos Andrade: implementaÃ§Ã£o das projeÃ§Ãµes, suite de testes e ADR-006

Ferramenta: Gemini.
Arquivos afetados: [`DisponibilidadeProjecaoService.java`](../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/DisponibilidadeProjecaoService.java),
[`ReconstrucaoService.java`](../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/ReconstrucaoService.java),
[`ReconstrucaoDeProjecaoTest.java`](../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/service/ReconstrucaoDeProjecaoTest.java),
[`ADR-007`](adr/ADR-007-projecoes-e-replay.md) e [`aula-05.md`](entregas/aula-05.md).

---

#### InteraÃ§Ã£o 6 â€” auxÃ­lio com sintaxe Java, navegaÃ§Ã£o no IntelliJ, revisÃ£o da projeÃ§Ã£o e ADR-006

**Pedido:** OrientaÃ§Ã£o para superar dificuldades de adaptaÃ§Ã£o ao ecossistema Java (sintaxe do Spring/Stream API) e Ã  navegaÃ§Ã£o no IntelliJ IDEA; revisÃ£o da lÃ³gica de projeÃ§Ã£o/replay implementada; criaÃ§Ã£o da suÃ­te de testes de integraÃ§Ã£o; e formataÃ§Ã£o da ADR-006 no padrÃ£o da equipe.

**Sugerido pela IA:**
1. Apresentar explicaÃ§Ãµes detalhadas passo a passo do cÃ³digo Java, alÃ©m de atalhos e fluxos do IntelliJ para execuÃ§Ã£o de testes unitÃ¡rios e depuraÃ§Ã£o.
2. SugestÃ£o de criar novos arquivos e testes sem reaproveitar a estrutura existente no projeto.

**RECUSADO:**
- A criaÃ§Ã£o de arquivos fora do padrÃ£o e testes desvinculados do contexto original foram recusadas para nÃ£o quebrar as regras de pacotes estabelecidas no `AGENTS.md`.

**Adotado:**
- UtilizaÃ§Ã£o da IA para traduzir o funcionamento do cÃ³digo Java do Spring/JDBC e guiar a execuÃ§Ã£o dos testes pelo IntelliJ.
- RefatoraÃ§Ã£o e revisÃ£o da lÃ³gica da classe `ReconstrucaoService` para garantir que o replay zerasse a tabela e o checkpoint de forma atÃ´mica e determinÃ­stica.
- CriaÃ§Ã£o do teste de integraÃ§Ã£o `ReconstrucaoDeProjecaoTest.java`, garantindo que o reprocessamento do log reconstrÃ³i o estado exato da projeÃ§Ã£o, corrige ediÃ§Ãµes indevidas no banco e consome novos fatos agendados.
- FormataÃ§Ã£o final do arquivo `ADR-006-projecoes-e-replay.md` seguindo rigorosamente a estrutura, tom tÃ©cnico e padrÃµes dos ADRs anteriores da equipe.

---

## Projeto final

### Maria LuÃ­sa Lacerda (257115): Saga do pagamento recusado

Ferramenta: ChatGPT.

Arquivos afetados: [`IngressoLiberadoEvent.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/domain/IngressoLiberadoEvent.java),
[`PagamentoRecusadoEvent.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/domain/PagamentoRecusadoEvent.java),
[`VendaService.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/service/VendaService.java),
[`PagamentoListener.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/controller/PagamentoListener.java),
[`EstoqueDoSetor.java`](../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/domain/EstoqueDoSetor.java),
[`IngressoService.java`](../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/IngressoService.java),
[`IngressoServiceIdempotenciaTest.java`](../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/service/IngressoServiceIdempotenciaTest.java),
[`ReconstrucaoDeProjecaoTest.java`](../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/service/ReconstrucaoDeProjecaoTest.java),
[`contrato.md`](contrato.md),
[`arquitetura.md`](arquitetura.md) e
[`ADR-006`](adr/ADR-006-resiliencia.md).

---

#### InteraÃ§Ã£o 1: modelagem da Saga do pagamento recusado

**Pedido:** substituir o fluxo antigo de compensaÃ§Ã£o direta por uma Saga coreografada para representar a recusa do pagamento e a posterior liberaÃ§Ã£o da reserva.

**Sugerido:** separar os fatos em `PagamentoRecusadoEvent` e `IngressoLiberadoEvent`. O gateway de pagamento simulado publica a recusa; o `servico-vendas` consome esse fato, localiza a reserva correspondente e publica a liberaÃ§Ã£o; o `servico-ingressos` consome a liberaÃ§Ã£o e devolve o estoque.

**Aceito:** o fluxo antigo baseado em `IngressoReservaCompensadaEvent` foi substituÃ­do pela sequÃªncia `PagamentoRecusadoEvent` â†’ `IngressoLiberadoEvent`. O evento de liberaÃ§Ã£o mantÃ©m `reservaEventoId` para identificar a reserva desfeita e `pagamentoEventoId` para correlacionar a liberaÃ§Ã£o com a recusa que a originou.

---

#### InteraÃ§Ã£o 2: devolver somente o que a reserva retirou

**Pedido:** definir como o `servico-ingressos` deveria processar uma liberaÃ§Ã£o sem confiar na quantidade recebida no comando de liberaÃ§Ã£o e sem devolver ingressos que a reserva nÃ£o chegou a retirar.

**Sugerido:** reconstruir `EstoqueDoSetor` a partir do stream e manter a relaÃ§Ã£o entre `reservaEventoId` e a quantidade efetivamente retirada. A liberaÃ§Ã£o consulta esse histÃ³rico e gera `IngressoDevolvidoEvent` somente com a quantidade que aquela reserva realmente retirou.

**Aceito:** `EstoqueDoSetor` passou a reconstruir as retiradas por reserva e a liberaÃ§Ã£o usa o histÃ³rico como fonte da verdade. Uma reserva recusada ou jÃ¡ liberada nÃ£o produz nova devoluÃ§Ã£o. Quando a liberaÃ§Ã£o chega antes da reserva correspondente, Ã© lanÃ§ada `ReservaAindaNaoProcessadaException`, permitindo tratar o caso como falha transitÃ³ria.

---

#### InteraÃ§Ã£o 3: idempotÃªncia e reconstruÃ§Ã£o do estoque

**Pedido:** adaptar os testes existentes ao novo fluxo de liberaÃ§Ã£o e verificar que entregas repetidas nÃ£o devolvem ingressos mais de uma vez.

**Sugerido:** testar a mesma liberaÃ§Ã£o entregue trÃªs vezes, duas liberaÃ§Ãµes diferentes para a mesma reserva, liberaÃ§Ã£o de reserva recusada, liberaÃ§Ã£o antes da reserva e reconstruÃ§Ã£o da projeÃ§Ã£o apÃ³s uma retirada seguida de devoluÃ§Ã£o.

**Aceito:** os testes passaram a verificar a idempotÃªncia tanto pelo `eventoId` processado quanto pela reserva que estÃ¡ sendo desfeita. O cenÃ¡rio de reconstruÃ§Ã£o foi ajustado para preservar o resultado esperado da PISTA depois de reservar e liberar ingressos. Os testes do `servico-vendas` passaram com 19 testes executados, sem falhas ou erros, e os testes de liberaÃ§Ã£o do `servico-ingressos` tambÃ©m passaram nos cenÃ¡rios implementados.

---

#### InteraÃ§Ã£o 4: contrato e documentaÃ§Ã£o da nova Saga

**Pedido:** atualizar a documentaÃ§Ã£o sem manter o contrato antigo de compensaÃ§Ã£o e garantir que os documentos refletissem os eventos realmente implementados.

**Sugerido:** reescrever as seÃ§Ãµes do contrato referentes Ã  compensaÃ§Ã£o como contrato de `IngressoLiberadoEvent`, documentar `PagamentoRecusadoEvent`, registrar as correlaÃ§Ãµes `eventoId`, `reservaEventoId` e `pagamentoEventoId` e atualizar o desenho arquitetural com os trÃªs tÃ³picos da Saga.

**Aceito:** `docs/contrato.md` passou a documentar `vendas.ingresso.liberado.v1` e `pagamentos.pagamento.recusado.v1`; `docs/arquitetura.md` passou a apresentar o domÃ­nio, os eventos publicados e internos, a topologia Kafka, o fluxo da Saga e as decisÃµes registradas nos ADRs. Antes de finalizar o contrato, a implementaÃ§Ã£o de `IngressoLiberadoEvent` foi conferida para confirmar que `pagamentoEventoId` jÃ¡ fazia parte do evento publicado.
