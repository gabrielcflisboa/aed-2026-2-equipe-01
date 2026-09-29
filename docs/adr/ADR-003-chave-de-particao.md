# ADR-003: a chave de partição do tópico de reservas

## Status

Aceita · 2026-09-29 · Equipe 01

Renumerado de ADR-004 para ADR-003 em 29/09/2026, sem mudança de conteúdo, para ocupar o
caminho exato que o enunciado da aula 04 pede (`docs/adr/ADR-003-chave-de-particao.md`).
O número 003 abriu porque a Aula 03 não exige ADR nenhum — o `ADR-003-contrato-agregador-e-compensacao.md`
que ocupava esse número era iniciativa da equipe, não exigência do enunciado, e virou
[ADR-004](ADR-004-contrato-agregador-e-compensacao.md). Mesma manobra que o
[ADR-007](ADR-007-projecoes-e-replay.md) já fez antes, para abrir espaço ao ADR-006 de
resiliência.

Entrega da aula 04. Revê, sob o critério que a aula ensinou, uma decisão que já estava
tomada desde o [ADR-005](ADR-005-event-sourcing.md) e o [contrato.md](../contrato.md#4-chave-de-particionamento):
a chave de partição do tópico `vendas.ingresso.reservado.v1` é o campo `evento`. Este ADR
não muda a chave — justifica por que ela é a certa, e nomeia o que ela deixou de responder.

## Contexto

A aula 04 ensinou quatro perguntas para escolher uma chave de partição: qual é a menor
unidade cuja ordem o negócio exige; por qual dimensão o negócio pergunta o tempo todo; se
alguma chave concentra volume demais; e quantas partições, sabendo que aumentar depois
redistribui as chaves e quebra a ordem. A demonstração da aula mostrou o caso em que a
primeira e a segunda pergunta apontam para chaves diferentes — a ordem é por pedido, a
pergunta do negócio é por cliente — e o preço de resolver isso é um repartition topic.

O `servico-ingressos` já precisava de uma resposta para a primeira pergunta desde o
ADR-005: o agregado `EstoqueDoSetor` é um stream por `(evento, setor)`, e a invariante do
domínio (não vender mais que a capacidade) só se resolve dentro desse par. A aula 04 é a
oportunidade de escrever explicitamente por que a chave escolhida no tópico serve a esse
agregado, e o que ela custaria se a pergunta do negócio fosse outra.

## Decisão

A chave de partição do tópico `vendas.ingresso.reservado.v1` é o campo `evento` (o
identificador do show/evento de entretenimento).

**A pergunta que ela responde sem repartir:** duas reservas do mesmo evento, para o mesmo
ou para setores diferentes, chegam na mesma partição e são processadas em ordem. É essa
ordem que permite ao `IngressoService` reconstruir o stream `(evento, setor)` e decidir
aceitar ou recusar uma reserva sem depender de coordenação entre partições — dois
compradores disputando a PISTA do mesmo show nunca ficam em partições diferentes disputando
sem se ver. Pelo mesmo motivo, o agregador da aula 03
(`AgregacaoDeReservasService`, soma por `evento` + `setor` numa janela de 1 minuto) também
não precisa de repartition topic: a dimensão que ele agrega (`evento`) é a mesma da chave.

Nesta etapa do projeto, a pergunta 1 (ordem que o negócio exige) e a pergunta 2 (dimensão
que o negócio pergunta o tempo todo) apontam para a mesma chave, `evento` — diferente da
demonstração da aula, em que as duas divergiam. É por isso que o projeto, até aqui, nunca
precisou de um repartition topic.

**A pergunta que ela deixou de responder sem repartir:** quantos ingressos um mesmo CPF
reservou, somando todos os eventos. Como a chave é `evento` e não `cpfComprador`, duas
reservas do mesmo comprador para shows diferentes caem em partições diferentes — não há
garantia de ordem nem de colocação entre elas. Hoje isso não é um problema porque o limite
por CPF (`VendaConfig.limitePorCpf`) é verificado em memória, dentro do `servico-vendas`,
antes de qualquer publicação no Kafka — não depende de reler o tópico. Mas se essa pergunta
precisasse virar um relatório de fluxo (por exemplo, "quantos ingressos por CPF, ao vivo,
somando todos os eventos"), o projeto pagaria a mesma repartição que a aula demonstrou: um
`RepartidorDeReservasListener` que republica os mesmos eventos, intocados, num tópico novo
chaveado por `cpfComprador`, e um agregador novo lendo esse tópico.

## Alternativas consideradas

**Chave = `compraId`.** Cada reserva teria sua própria posição, sem relação com nenhuma
outra. Recusada: duas reservas de compras diferentes disputando o mesmo setor do mesmo
evento poderiam cair em partições diferentes e ser processadas sem ordem entre si — a
invariante de capacidade do ADR-005 deixaria de ser garantida pela ordem da partição, e
voltaria a depender só do `ConcorrenciaNoStreamException` para pegar o que a ordem já
evitaria sozinha.

**Chave = `setor`, sem o `evento`.** Pareceria razoável, já que o agregado também é por
setor. Recusada: `setor` sozinho agruparia, na mesma partição, a "PISTA" de eventos
completamente diferentes, que não disputam nada entre si — é o mesmo erro que a aula
descreveu para "o evento inteiro como agregado" no ADR-005, só que na direção oposta: uma
concentração que o domínio não pede.

**Chave = `cpfComprador`.** Resolveria a pergunta por CPF sem repartir, mas romperia a que
mais importa: duas reservas do mesmo evento e setor, de compradores diferentes, poderiam
cair em partições diferentes e perder a ordem que impede o overselling. Trocaria o problema
que o domínio realmente tem pelo que ele não tem.

## Consequências aceitas

**Um relatório "ingressos por CPF, em todos os eventos" não existe sem repartir.** Não é um
problema hoje — a checagem de limite é síncrona e em memória, no `servico-vendas`, antes da
publicação. Se um dia isso precisar virar um relatório de fluxo (streaming, não só a
checagem síncrona), o custo é o do repartition topic: mais um serviço, mais um tópico com
retenção própria, mais uma passagem de latência, at-least-once de novo (o consumidor final
continua precisando ser idempotente), e o cliente com maior volume concentrando trabalho
numa partição só.

**Um evento com volume muito maior que os outros concentra uma partição.** Se um show
vender ordens de grandeza mais que os demais, a partição dele fica perto do limite enquanto
as outras partições do mesmo tópico ficam ociosas — a terceira pergunta da aula 04
(concentração de volume) fica sem resposta automática. Não implementamos nada para isso
nesta etapa; o gatilho para agir seria o lag daquela partição específica crescer sob carga
real, não um número estimado agora.

**Aumentar o número de partições depois redistribui as chaves e quebra a ordem.** Se o
tópico precisar de mais partições para dar mais paralelismo ao consumidor, o hash de cada
`evento` muda de partição, e a garantia de ordem que hoje vale para um evento em andamento
se perde no meio da migração. Isso teria que ser feito numa janela sem reservas em
andamento, não a qualquer momento.

**A resposta depende do agregado já estar certo.** Esta decisão de chave só funciona porque
o agregado do ADR-005 já é `(evento, setor)` — a chave replica a fronteira de consistência
que o domínio já tinha. Se o agregado mudasse, a chave teria que mudar junto, e este ADR
ficaria desatualizado.
