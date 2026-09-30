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

## Aula 03

### Gabriel Campos Ferreira Lisboa (255696) — agregador por janela de tempo

Ferramenta: GitHub Copilot.
Arquivos afetados: `AgregadorDeReservasListener.java`,
`AgregacaoDeReservasService.java`, `AgregacaoJdbcRepository.java`,
`IngressoReservadoEvent.java` (adição de `reservadoEm`), `schema.sql`,
`AgregacaoDeReservasServiceTest.java`.

**Pedido:** implementar um segundo consumidor, com `group.id` próprio, que
agregasse o fluxo de reservas por janela de tempo, respondendo a uma
pergunta de negócio.

**Sugerido:** usar processing time (`Instant.now()` no momento em que o
listener recebe a mensagem) para calcular a janela, por ser mais simples de
implementar e não depender de nenhum campo do payload.

**RECUSADO:** processing time para esta pergunta. A pergunta agregada é
"quantos ingressos foram reservados por setor/evento" — um fato do domínio,
não da infraestrutura de consumo. Com processing time, reprocessar o tópico
do início (por exemplo depois de corrigir um bug no agregador) mudaria o
resultado, já que cada mensagem cairia em uma janela diferente dependendo de
quando fosse lida. Isso tornaria a agregação não confiável como fonte de
relatório.

**Adotado:** event time, lendo `reservadoEm` do próprio payload (campo que já
existe no evento do publisher, mas que o consumidor original — por ser
tolerante e minimalista — não declarava). O consumidor da aula 02 continua
ignorando esse e outros campos que não usa; só o novo evento consumido pelo
agregador passou a declarar `reservadoEm`. Isso torna o resultado da
agregação determinístico sob reprocessamento, o que o teste
`AgregacaoDeReservasServiceTest` confirma diretamente ao gravar eventos fora
de ordem de chegada e verificar que cada um cai na janela correta.

---

### Amir Gabriel Dantas Santos Andrade (1666035) — compensação por pagamento recusado

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

#### Interação 1 — como publicar o evento de compensação

**Pedido:** ligar o `IngressoReservaCompensadaEvent` ao Kafka, já que o `KafkaTemplate` existente era
`KafkaTemplate<String, IngressoReservadoEvent>`.

**Sugerido:** generalizar para um único `KafkaTemplate<String, Object>`, com tópico, `ce_type`, id e
instante virando parâmetros do `publicar(...)`, porque o Kafka só transporta bytes e a tipagem existe
apenas no compilador Java.

**Aceito:** a explicação de que a tipagem não existe no Kafka, só no lado do `servico-vendas`, e portanto
não centraliza nada nem é risco entre serviços.

**RECUSADO:** o `KafkaTemplate<String, Object>` genérico. A equipe preferiu o padrão que o projeto já usa
(um template tipado por evento): dá segurança em tempo de compilação, sabemos exatamente qual evento cada
serviço de publicação envia, e não dá para publicar o evento errado no tópico errado.

**Adotado:** segundo bean `kafkaTemplateCompensacao` no `VendaConfig` e classe irmã
`VendaCompensacaoCallbackService`. O `VendaCallbackService` e o teste dele não foram alterados.

---

#### Interação 2 — quem decide a recusa do pagamento

**Pedido:** simular o pagamento recusado que dispara a compensação.

**Sugerido:** endpoint `POST /vendas/reservas/{compraId}/compensacoes` chamando direto
`VendaService.compensarPagamentoRecusado(compraId)`, que já assumia que a recusa aconteceu.

**RECUSADO:** deixar a decisão de recusar dentro do `VendaService`. Um gateway de pagamento é um sistema
externo; a lógica dele não pode morar na regra de negócio do `servico-vendas`, que deve apenas reagir a uma
recusa ocorrida em outro lugar.

**Adotado:** `GatewayDePagamentoService` simula o gateway e devolve o motivo; o controller liga as duas
peças. O campo `motivo` passou a viajar no evento até o `IngressoDevolvidoEvent`, em vez de um texto fixo
no consumidor. O `reservasAceitas` (mapa em memória) é a simplificação da simulação: num cenário real seria
uma tabela de compras do próprio `servico-vendas`.

---

#### Interação 3 — identidade do evento de compensação e idempotência

**Pedido:** consumir a compensação no `servico-ingressos` devolvendo o estoque.

**Sugerido:** ao ler o código, a IA apontou dois problemas: o `contrato.md` mandava reusar o `eventoId` da
reserva original no evento de compensação, e o `IngressoService.compensar(...)` não registrava deduplicação
(registrar por item quebraria a partir do segundo item da mesma mensagem).

**RECUSADO:** reaproveitar o `eventoId` da reserva. A tabela `evento_processado` usa `eventoId` como chave e
é compartilhada; a compensação seria descartada como "já processada" e a devolução sumiria sem erro.

**Adotado:** `eventoId` novo por compensação, com o `contrato.md` corrigido (seções 9 a 12 e 14, incluindo
`compraId`, `evento`, `motivo` e `compensadoEm`). O `processarCompensacao(...)` registra a deduplicação uma
vez por mensagem e reaproveita o `compensar(...)` por item. O teste entrega a mesma compensação 3 vezes e
confere um único `IngressoDevolvido`. Verificado ponta a ponta com Kafka real: reserva, recusa, e o estoque
voltou a 100.

---

## Aula 04

### Amir Gabriel Dantas Santos Andrade (1666035) — chave de partição e janela do agregador

Ferramenta: Claude (Claude Code).
Arquivos afetados: [`ADR-003-chave-de-particao.md`](adr/ADR-003-chave-de-particao.md) e
[`aula-04.md`](entregas/aula-04.md).

---

#### Interação 1 — por que a janela do agregador continua sendo tumbling

**Pedido:** revisar, com o vocabulário desta aula (tumbling/hopping/sliding/session), a
janela de 1 minuto que o agregador da aula 03 já usava.

**Sugerido:** considerar hopping, para dar uma leitura de tendência (média móvel) além do
total por minuto.

**RECUSADO.** A pergunta que o agregador responde (`GET /agregacao/reservas-por-setor`) é
"quantos ingressos foram reservados por setor, nesse minuto" — um total de período, não
uma tendência. Com hopping, a mesma reserva entraria em mais de uma janela, e a soma das
janelas deixaria de bater com o total de ingressos realmente reservados — o mesmo problema
que o slide da aula descreve com o faturamento saindo 5 vezes maior que o real, sem erro
nenhum acusando isso.

**Adotado:** manter tumbling, sem mudança de código. A justificativa comparando as quatro
janelas ficou registrada em `docs/entregas/aula-04.md`.

---

## Aula 05

### Pedro Assis Corrêa (256357): event sourcing do estoque

Ferramenta: Claude (Claude Code).
Arquivos afetados: todo o `servico-ingressos`, com agregado, event store e
testes; [`ADR-005`](adr/ADR-005-event-sourcing.md);
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
direto na tabela é estado que o log não conhece: o agregado se reconstrói só a
partir dos eventos do stream, então nasceria com capacidade zero e recusaria toda
reserva. O `data.sql` ficaria lá, correto e ignorado.

**Adotado:** a abertura do setor virou o primeiro fato do stream
(`SetorAbertoEvent`), gravado pelo `AberturaDeSetoresService` no arranque, uma vez
por stream. A capacidade passa a sobreviver a qualquer replay porque ela *é*
parte do log.

---

#### Interação 3: de onde a decisão lê o estoque

**Pedido:** o `IngressoService` relê o stream inteiro a cada mensagem para saber
quanto resta. Dava para manter uma tabela `disponibilidade_por_setor` com o saldo
já calculado e ler uma linha só?

**Sugerido:** sim, uma projeção derivada do log, com a coluna `disponivel` já
pronta. A decisão viraria uma consulta de uma linha em vez de um replay.

**RECUSADO.** É a armadilha exata que o enunciado descreve. No momento em que a
decisão de aceitar ou recusar depende de uma tabela derivada, essa tabela vira
fonte da verdade sem ninguém ter decidido isso, e o atraso da atualização, que
seria um inconveniente visual, passa a vender o mesmo assento duas vezes. Quem
decide é o agregado, reconstruído do stream.

A recusa também define o lugar da primeira projeção quando ela entrar: tela, e
nada além disso. Por isso esta entrega para no event store, e a folha de entrega
registra a única leitura que existe hoje, com defasagem zero.

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

#### Interação 5: a sugestão que o teste derrubou

Registro pelo valor de método. Foi aceita, entrou no código e só caiu quando a
suíte rodou.

**`abertura: {}`** no `application.yml` de teste, para dizer "mapa vazio". O
binder do Spring lê isso como a *string* `"{}"` e falha com
`ConverterNotFoundException`. A correção foi omitir a chave, porque o campo já
nasce como `LinkedHashMap` vazio.

---

### Amir Gabriel Dantas Santos Andrade: implementação das projeções, suite de testes e ADR-006

Ferramenta: Gemini.
Arquivos afetados: [`DisponibilidadeProjecaoService.java`](../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/DisponibilidadeProjecaoService.java),
[`ReconstrucaoService.java`](../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/ReconstrucaoService.java),
[`ReconstrucaoDeProjecaoTest.java`](../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/service/ReconstrucaoDeProjecaoTest.java),
[`ADR-007`](adr/ADR-007-projecoes-e-replay.md) e [`aula-05.md`](entregas/aula-05.md).

---

#### Interação 6 — auxílio com sintaxe Java, navegação no IntelliJ, revisão da projeção e ADR-006

**Pedido:** Orientação para superar dificuldades de adaptação ao ecossistema Java (sintaxe do Spring/Stream API) e à navegação no IntelliJ IDEA; revisão da lógica de projeção/replay implementada; criação da suíte de testes de integração; e formatação da ADR-006 no padrão da equipe.

**Sugerido pela IA:**
1. Apresentar explicações detalhadas passo a passo do código Java, além de atalhos e fluxos do IntelliJ para execução de testes unitários e depuração.
2. Sugestão de criar novos arquivos e testes sem reaproveitar a estrutura existente no projeto.

**RECUSADO:**
- A criação de arquivos fora do padrão e testes desvinculados do contexto original foram recusadas para não quebrar as regras de pacotes estabelecidas no `AGENTS.md`.

**Adotado:**
- Utilização da IA para traduzir o funcionamento do código Java do Spring/JDBC e guiar a execução dos testes pelo IntelliJ.
- Refatoração e revisão da lógica da classe `ReconstrucaoService` para garantir que o replay zerasse a tabela e o checkpoint de forma atômica e determinística.
- Criação do teste de integração `ReconstrucaoDeProjecaoTest.java`, garantindo que o reprocessamento do log reconstrói o estado exato da projeção, corrige edições indevidas no banco e consome novos fatos agendados.
- Formatação final do arquivo `ADR-007-projecoes-e-replay.md` seguindo rigorosamente a estrutura, tom técnico e padrões dos ADRs anteriores da equipe.

---

## Projeto final

### Maria Luísa Lacerda (257115): Saga do pagamento recusado

Ferramenta: ChatGPT.

Arquivos afetados: [`IngressoLiberadoEvent.java`](../servico-vendas/src/main/java/br/pucminas/aed/vendas/domain/IngressoLiberadoEvent.java), [`EstoqueDoSetor.java`](../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/domain/EstoqueDoSetor.java), [`contrato.md`](contrato.md), [`arquitetura.md`](arquitetura.md) e [`ADR-006-resiliencia.md`](adr/ADR-006-resiliencia.md).

#### Interação 1: modelagem da Saga do pagamento recusado

**Pedido:** substituir o fluxo antigo de compensação direta por uma Saga coreografada para representar a recusa do pagamento e a posterior liberação da reserva.

**Sugerido:** separar os fatos em `PagamentoRecusadoEvent` e `IngressoLiberadoEvent`. O gateway de pagamento simulado publica a recusa; o `servico-vendas` consome esse fato, localiza a reserva correspondente e publica a liberação; o `servico-ingressos` consome a liberação e devolve o estoque.

**Aceito:** o fluxo antigo baseado em `IngressoReservaCompensadaEvent` foi substituído pela sequência `PagamentoRecusadoEvent` → `IngressoLiberadoEvent`. O evento de liberação mantém `reservaEventoId` para identificar a reserva desfeita e `pagamentoEventoId` para correlacionar a liberação com a recusa que a originou.

#### Interação 2: devolver somente o que a reserva retirou

**Pedido:** definir como o `servico-ingressos` deveria processar uma liberação sem confiar na quantidade recebida no comando de liberação e sem devolver ingressos que a reserva não chegou a retirar.

**Sugerido:** reconstruir `EstoqueDoSetor` a partir do stream e manter a relação entre `reservaEventoId` e a quantidade efetivamente retirada. A liberação consulta esse histórico e gera `IngressoDevolvidoEvent` somente com a quantidade que aquela reserva realmente retirou.

**Recusado:** usar a quantidade informada pelo evento de liberação como valor a ser devolvido ao estoque. Essa abordagem permitiria devolver ingressos que a reserva não chegou a retirar, repetindo o problema existente no mecanismo anterior de compensação. A quantidade devolvida deve ser determinada pelo histórico do agregado, relacionando `reservaEventoId` à retirada efetivamente registrada para aquela reserva.

**Aceito:** `EstoqueDoSetor` passou a reconstruir as retiradas por reserva e a liberação usa o histórico como fonte da verdade. Uma reserva recusada ou já liberada não produz nova devolução. Quando a liberação chega antes da reserva correspondente, é lançada `ReservaAindaNaoProcessadaException`, permitindo tratar o caso como falha transitória.

#### Interação 3: idempotência e reconstrução do estoque

**Pedido:** adaptar os testes existentes ao novo fluxo de liberação e verificar que entregas repetidas não devolvem ingressos mais de uma vez.

**Sugerido:** testar a mesma liberação entregue três vezes, duas liberações diferentes para a mesma reserva, liberação de reserva recusada, liberação antes da reserva e reconstrução da projeção após uma retirada seguida de devolução.

**Aceito:** os testes passaram a verificar a idempotência tanto pelo `eventoId` processado quanto pela reserva que está sendo desfeita. O cenário de reconstrução foi ajustado para preservar o resultado esperado da PISTA depois de reservar e liberar ingressos. Os testes do `servico-vendas` passaram com 19 testes executados, sem falhas ou erros, e os testes de liberação do `servico-ingressos` também passaram nos cenários implementados.

#### Interação 4: contrato e documentação da nova Saga

**Pedido:** atualizar a documentação para refletir a substituição do mecanismo antigo de compensação pela Saga do pagamento recusado.

**Sugerido:** reescrever as seções do contrato referentes à compensação como contrato de `IngressoLiberadoEvent`, documentar `PagamentoRecusadoEvent`, registrar as correlações `eventoId`, `reservaEventoId` e `pagamentoEventoId` e atualizar o desenho arquitetural com os três tópicos da Saga.

**Aceito:** `docs/contrato.md` passou a documentar `vendas.ingresso.liberado.v1` e `pagamentos.pagamento.recusado.v1`; `docs/arquitetura.md` passou a apresentar o domínio, os eventos publicados e internos, a topologia Kafka, o fluxo da Saga e as decisões registradas nos ADRs. Antes de finalizar o contrato, a implementação de `IngressoLiberadoEvent` foi conferida para confirmar que `pagamentoEventoId` já fazia parte do evento publicado.

### Pedro Assis Corrêa (256357): caminho de falha, reprocessamento e documento de arquitetura

Ferramenta: Claude (Claude Code).

Arquivos afetados: [`ResilienciaConfig.java`](../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/ResilienciaConfig.java), [`ReprocessamentoService.java`](../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/ReprocessamentoService.java), [`CaminhoDeFalhaTest.java`](../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/CaminhoDeFalhaTest.java), [`ADR-006-resiliencia.md`](adr/ADR-006-resiliencia.md), [`arquitetura.md`](arquitetura.md), [`contrato.md`](contrato.md) e [`AGENTS.md`](../AGENTS.md).

#### Interação 1: onde fica o ADR da Saga

**Pedido:** montar, a partir das notas de aula de 14/09 e do estado da branch, a lista do que faltava para a entrega de 29/09.

**Sugerido:** criar o ADR da Saga como `docs/adr/ADR-007-...md`, porque o número 006 já estava ocupado pelo ADR de projeções, e criar uma folha de entrega em `docs/entregas/aula-06.md`.

**RECUSADO:** as duas sugestões. O enunciado do projeto final fixa o caminho `docs/adr/ADR-006-resiliencia.md` e avisa que caminho diferente é caminho não encontrado; ele também não pede folha de entrega desta etapa. A ferramenta partiu de um acordo antigo da equipe e não tinha o enunciado. Quando o PDF entrou na conversa, as duas sugestões caíram.

**Aceito:** renumerar o ADR de projeções para ADR-007, sem mudar o texto, e criar o ADR-006 no caminho exigido. O ADR-006 registra essa troca na seção Status.

#### Interação 2: o que a devolutiva mudava no plano

**Pedido:** refazer o plano como lista técnica de arquivos, levando em conta a devolutiva de 03/09.

**Sugerido:** publicar `PagamentoRecusadoEvent` pelo gateway simulado, trocar `IngressoReservaCompensadaEvent` por `IngressoLiberadoEvent` com `reservaEventoId`, e tornar o agregador idempotente, porque reprocessar pela DLQ entregaria o evento de novo a ele. A leitura do código mostrou que a compensação de uma reserva recusada devolvia ingressos de outro comprador.

**Aceito:** as mudanças entraram no plano, que foi dividido entre duas pessoas. A consulta `GET /estoque` ficou com o caminho de falha, porque é por ela que se vê o efeito desfeito.

#### Interação 3: implementação do caminho de falha

**Pedido:** implementar a parte do plano que ficou comigo.

**Sugerido:** `DefaultErrorHandler` com `DeadLetterPublishingRecoverer`, espera exponencial limitada e lista de exceções permanentes; endpoint de reprocessamento por `ce_id`; expurgo diário da deduplicação; e um teste com Kafka embutido que simula o banco fora do ar com `@MockitoSpyBean` no `DeduplicacaoRepository`.

**Aceito:** o código entrou depois de compilado e testado contra as dependências do projeto (Spring Boot 4.1.0, Spring Kafka 4.1, Jackson 3). Os 42 testes do `servico-ingressos` passam, incluindo os quatro cenários do `CaminhoDeFalhaTest`. Durante a implementação, a ferramenta também encontrou este arquivo com a codificação corrompida nos registros das aulas 02 a 05 e restaurou o texto a partir da última versão íntegra.
