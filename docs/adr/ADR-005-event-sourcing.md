# ADR-005: event sourcing no estoque de ingressos

## Status

Aceita · 2026-09-02 · Equipe 01

Sucede parcialmente o [ADR-002](ADR-002-dominio-do-projeto.md), que escolheu o
domínio. Este ADR decide onde o event sourcing entra e o que deixa de ser fonte
da verdade por causa dele.

## Contexto

Até a aula 04 o `servico-ingressos` guardava o estoque numa tabela mutável
(`estoque_setor`), com uma linha por setor e um `UPDATE` a cada reserva. Isso
respondia "quantos restam" e mais nada:

- Não dava para saber por que um setor tinha 8 e não 10. O `UPDATE` destruiu o
  caminho até o número.
- A tabela era a única cópia da verdade. Um bug num `UPDATE` corrompia o estoque
  sem deixar rastro, e não havia de onde recuperar.
- Perguntas novas exigiam colunas novas. "Quantas pessoas tentaram comprar
  depois de esgotar", que o ADR-002 lista como algo que vale reprocessar, era
  irrespondível: a tentativa recusada nunca foi gravada em lugar nenhum.

O intervalo entre reservar e confirmar o pagamento, que o ADR-002 apontou como o
lugar onde os sistemas de venda de ingresso falham, é exatamente o intervalo em
que precisamos saber o histórico e não só o saldo.

## Decisão

O agregado é `EstoqueDoSetor`, e o stream é o par `(evento, setor)`.

O estado do estoque passa a ser derivado de um log append-only
(`evento_do_estoque`). Nenhuma parte do sistema faz `UPDATE` ou `DELETE` nesse
log. A única escrita é `INSERT`.

### Por que este agregado

O agregado é a fronteira de consistência, o menor conjunto de dados que precisa
ser decidido junto. A invariante que este domínio tem é uma só, não vender mais
ingressos do que a capacidade do setor, e ela se resolve inteiramente dentro de
um setor de um evento. Nenhuma decisão sobre a PISTA precisa olhar o CAMAROTE, e
nenhuma decisão sobre o show de agosto precisa olhar o de setembro.

O [contrato](../contrato.md#4-chave-de-partição) já dizia isso ao justificar a
chave de partição `evento`: as reservas de um mesmo evento disputam
disponibilidade por setor. O agregado é o refinamento dessa mesma frase.

### A versão é o mecanismo de concorrência

Cada evento ocupa uma `versao` dentro do seu stream, e a tabela tem
`UNIQUE (stream_id, versao)`. Quem vai gravar informa a versão que leu ao
reconstruir o agregado. Se outro processo gravou nesse meio-tempo, a chave colide
e a gravação é recusada com `ConcorrenciaNoStreamException`.

Não há lock, não há `SELECT FOR UPDATE`. Duas reservas simultâneas para a mesma
PISTA lêem a versão 7, uma grava a 8 e a outra é recusada. Quem foi recusado relê
o stream e decide de novo, sobre o estado que agora vale. É o mecanismo que
impede dois compradores de levarem o mesmo assento, o problema que o ADR-002
usou para escolher este domínio.

### Quatro fatos, e a recusa é um deles

| Evento | O que registra |
|---|---|
| `SetorAbertoEvent` | o setor passou a existir com uma capacidade |
| `IngressoRetiradoEvent` | N ingressos saíram do estoque |
| `IngressoDevolvidoEvent` | N ingressos voltaram, a compensação do ADR-002 |
| `ReservaRecusadaEvent` | alguém pediu N e não cabia |

Gravar a recusa foi decisão deliberada. Um log que só registra sucesso responde
"quanto restou", mas fica mudo sobre quanta demanda existiu além da oferta, e
essa segunda pergunta é a que a produção do show quer. Como a recusa está no log,
ela pode ser projetada depois sem alterar o formato de nada.

A compensação é um fato novo, não um apagamento. O `IngressoDevolvidoEvent` anula
o efeito do `IngressoRetiradoEvent`, e os dois continuam no log. Depois de
qualquer reconstrução ainda dá para dizer que aquela reserva existiu e foi
desfeita, o que uma tabela mutável perderia.

### A capacidade também é um fato

A alternativa óbvia era popular a capacidade dos setores com um `data.sql`. Foi
recusada. Capacidade inserida direto na tabela é estado que o replay não
reproduz, e a primeira reconstrução zeraria o estoque inteiro. Como
`SetorAbertoEvent`, a capacidade é o primeiro evento do stream e sobrevive a
qualquer replay.

### As projeções não decidem nada

Duas projeções derivam do mesmo log:

- `disponibilidade_por_setor`, a tela do comprador
- `ocupacao_por_evento`, a tela da produção do show

Ambas são apagáveis a qualquer momento. E a regra que sustenta tudo: a decisão de
aceitar ou recusar uma reserva não lê projeção nenhuma. O `IngressoService` relê
o stream, reconstrói o agregado e decide sobre ele. Se a decisão lesse
`disponibilidade_por_setor`, um número atrasado deixaria de ser inconveniente
visual e passaria a permitir overselling, e a projeção teria virado fonte da
verdade sem ninguém ter decidido isso.

### Nota de nomenclatura

O [AGENTS.md](../../AGENTS.md) fixa quatro pacotes e uma lista fechada de
sufixos: `Application`, `Config`, `Controller`, `Listener`, `Service`,
`Repository`, `Event`, `VO`. Esta etapa não criou pacote novo. Tudo entrou em
`domain` e `service`.

Uma classe fica sem sufixo de propósito: `EstoqueDoSetor`. Nenhum item da lista
descreve uma raiz de agregado. `Service` diria que ela orquestra, e ela não tem
dependência nenhuma; `VO` diria que ela é um valor sem identidade, e ela tem
stream e versão. Como o padrão veda inventar sufixo fora da lista, a raiz do
agregado fica como substantivo de domínio puro, que é como o próprio negócio a
chama. `ConcorrenciaNoStreamException` segue a convenção que a aula 02 já usou em
`SetorIndisponivelException`.

## Alternativas consideradas

`Reserva` como agregado. Cada compra vira um stream com seu próprio ciclo
(reservada, paga, expirada). Recusada porque a invariante do domínio não está
dentro de uma reserva: uma reserva isolada não sabe se cabe. Com esse agregado, a
checagem de capacidade voltaria a depender de uma leitura externa, quase
certamente de uma projeção, que é o que queremos evitar. Este agregado
provavelmente volta na Saga, para o ciclo de vida do pagamento, mas não como dono
do estoque.

O evento de entretenimento inteiro como agregado. Um stream por show, cobrindo
todos os setores. Recusado por contenção: toda venda do show competiria pela
mesma versão, e num show grande a taxa de recusa por concorrência seria
proibitiva. Ganharíamos uma invariante que o domínio não pede, a capacidade total
do evento, ao custo de serializar vendas que não disputam nada entre si.

Manter a tabela mutável e só adicionar um log de auditoria ao lado. Recusado
porque duas verdades para o mesmo fato é pior do que uma verdade ruim: quando o
saldo e o log divergissem, não haveria critério para dizer qual vale. Se o log
não é a fonte, ele é decoração.

## Consequências aceitas

**Ler o estoque agora custa um replay do stream.** Cada mensagem relê o stream do
setor e reconstrói o agregado. Aceitamos porque o stream é curto, um evento por
reserva num setor. Não implementamos snapshots nesta etapa. O gatilho para
implementar será um stream passar de alguns milhares de eventos, e o desenho já
está preparado: o snapshot seria mais uma projeção, com a versão como checkpoint.

**As telas ficam atrasadas em relação ao log.** As projeções avançam por catch-up
periódico, então existe uma janela em que o log já tem o fato e a tela ainda não.
A defasagem tolerada por tela, com a justificativa de cada uma, está em
[docs/entregas/aula-05.md](../entregas/aula-05.md).

**O log só cresce.** Não há expurgo, e não deveria haver: apagar evento é apagar
a fonte da verdade. Retenção e arquivamento ficam fora do escopo desta etapa.

**Não há versionamento do formato dos eventos.** O discriminador gravado (`tipo`)
é um nome de domínio, não o nome da classe Java, para que uma refatoração não
invalide o log. Mas não há upcasting: se um campo mudar de significado, será
preciso um `IngressoRetiradoEvent` v2 e um caminho de leitura para os dois. Está
previsto, não está implementado.

**A exclusão mútua vale dentro de um banco.** A restrição `UNIQUE` protege
enquanto houver um H2. Vários nós contra bancos diferentes não têm garantia
nenhuma, o que é aceitável para o escopo da disciplina e precisaria mudar antes
de qualquer coisa parecida com produção.

**A tabela `evento_processado` continua mutável, e continua não sendo fonte da
verdade.** Ela guarda quais mensagens do broker já foram consumidas, para que uma
reentrega não vire dois fatos no log. Apagá-la não perde estoque: perde apenas a
proteção contra reprocessar mensagens antigas. É memória de entrega, não estado
de negócio, e por isso não entra no replay.

**O `servico-vendas` não foi migrado.** Ele continua sem persistir nada, com a
cota por CPF em memória. Não é event sourcing e não finge ser. Se essa cota
precisar sobreviver a reinício ou valer entre instâncias, será um segundo
agregado, com seu próprio ADR.
