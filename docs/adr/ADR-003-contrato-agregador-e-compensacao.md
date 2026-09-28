# ADR-003: contrato do evento, agregador por janela de tempo e compensação por pagamento recusado

## Status

Aceita · 2026-09-23 · Equipe 01

Sucede parcialmente o [ADR-002](ADR-002-dominio-do-projeto.md), que escolheu o domínio e
prometeu um caminho de compensação e algo que valesse reprocessar. Este ADR registra as
decisões da etapa 2 (aula 03) e da atualização feita depois na mesma branch, que fechou o
caminho de compensação. Foi escrito depois do código, e as decisões abaixo foram tomadas
durante a implementação; o [ADR-005](ADR-005-event-sourcing.md) e o
[ADR-007](ADR-007-projecoes-e-replay.md) são posteriores e já as pressupõem.

## Contexto

Ao fim da aula 02 o contrato do evento existia só de forma implícita, espalhado pelas
classes de cada lado, e o `servico-ingressos` tinha um único consumidor. Faltavam três coisas:

- Um documento que dissesse, para alguém de outro time, o que cada campo do evento
  **significa**, não apenas qual é o tipo dele.
- Uma leitura do fluxo que nenhum evento isolado responde: quantos ingressos foram reservados
  por setor, ao longo do tempo.
- O caminho de compensação que o ADR-002 listou como critério do domínio. O
  `IngressoReservaCompensadaEvent` e o `IngressoService.compensar(...)` existiam, mas nada
  publicava o evento e nenhum listener o consumia.

## Decisão

### O contrato é um documento, e a compatibilidade é BACKWARD

O contrato dos dois eventos vive em [contrato.md](../contrato.md): tipo do evento, tabela de
campos com tipo, obrigatoriedade e significado, formato de data (ISO-8601, nunca epoch), chave
de partição e exemplo de carga.

A regra de compatibilidade escolhida é **BACKWARD**: um consumidor novo lê o que o produtor
antigo escreveu. Na prática, o consumidor é implantado primeiro. Escolhemos BACKWARD porque os
dois serviços pertencem à mesma equipe e a ordem de implantação é coordenável, e porque o
consumidor já é tolerante (`@JsonIgnoreProperties(ignoreUnknown = true)`), então campo novo no
produtor não o quebra. Não subimos schema registry: o contrato é o artefato, o registry seria
só uma forma de fazê-lo cumprir.

A coluna que mais importa é a do **significado**, porque é a que nenhuma ferramenta verifica.
Se `precoUnitario` passasse a incluir taxa de serviço, o tipo continuaria `number` e o
consumidor somaria outra coisa sem erro nenhum. Por isso o contrato descreve o significado em
frase e trata mudança de significado como nova versão do tipo do evento.

### O agregador usa a hora de ocorrência (event time)

Um segundo consumidor (`AgregadorDeReservasListener`) lê o mesmo tópico de reservas com
`group.id` próprio (`servico-ingressos-agregador-reservas`), independente do consumidor
idempotente. Cada consumidor tem o seu offset, e os dois processam toda mensagem.

A pergunta de negócio é: **quantos ingressos foram reservados por setor, em cada evento, a
cada minuto**. A resposta fica na tabela `agregacao_reserva_por_setor_janela` e é observável em
`GET /agregacao/reservas-por-setor` e no log (com partição e offset).

Decisões de relógio:

| Ponto | Decisão | Por quê |
|---|---|---|
| Qual hora | **Ocorrência**: o campo `reservadoEm` do payload | A pergunta é sobre o ritmo real das reservas, não sobre quando o agregador leu a mensagem |
| Janela | 1 minuto, alinhada por relógio (`:00`, `:01`...) | O início da janela é o piso do `reservadoEm` em múltiplos de 60 s; nunca depende da hora em que o processo subiu |
| Evento atrasado | Atualiza (upsert) a janela a que o `reservadoEm` pertence | Sem watermark nesta etapa, uma janela antiga pode mudar depois de já ter sido lida |
| Reprocessar amanhã | Mesmo resultado, com a tabela zerada | A única entrada do cálculo é um dado do domínio que não muda |

O consumidor da aula 02 continua ignorando `reservadoEm`: só o agregador declara o campo.

### A compensação é reação a uma recusa que aconteceu fora do `servico-vendas`

O pagamento é recusado depois que o ingresso já saiu do estoque. Repetir o mesmo CPF não
simula isso: o limite por CPF recusa a reserva antes de qualquer evento existir, e não há nada
a devolver. São dois momentos distintos, e a compensação só faz sentido no segundo.

- **Gateway simulado separado.** `GatewayDePagamentoService` decide a recusa e devolve o
  motivo. O `VendaService` não decide nada: busca a reserva e publica o fato. O endpoint
  `POST /vendas/reservas/{compraId}/compensacoes` faz o papel do webhook que um gateway real
  chamaria de volta. A lógica de um sistema externo não mora na regra de negócio do serviço.
- **Um `KafkaTemplate` tipado por evento.** O Kafka só transporta bytes; a tipagem existe no
  compilador do `servico-vendas`. Ainda assim, seguimos o padrão que o projeto já usava: um
  template e um serviço de publicação por evento (`VendaCompensacaoCallbackService`, irmão do
  `VendaCallbackService`). Dá segurança em tempo de compilação: não dá para publicar o evento
  errado no tópico errado.
- **Tópico próprio** (`vendas.ingresso.reserva-compensada.v1`), para que cada contrato evolua e
  seja versionado sozinho e o consumidor não filtre tipos dentro de um mesmo fluxo.
- **`eventoId` novo por compensação**, nunca o da reserva original. A tabela de deduplicação
  (`evento_processado`) usa `eventoId` como chave e é compartilhada; reaproveitar o id da
  reserva faria a compensação ser descartada como "já processada", e a devolução sumiria sem
  erro. A ligação com a reserva é feita pelo `compraId`.
- **Deduplicação uma vez por mensagem.** `processarCompensacao(...)` registra o `eventoId` uma
  vez e depois devolve item a item. Registrar por item quebraria a partir do segundo item da
  mesma mensagem, porque a chave repetiria.
- **O motivo viaja no evento** (`motivo`) até o `IngressoDevolvidoEvent`, em vez de um texto
  fixo escrito no consumidor.

O `servico-vendas` guarda o que cada `compraId` reservou em memória (`reservasAceitas`), porque
sem isso não haveria o que devolver. Compensar a mesma compra duas vezes devolve 404.

## Alternativas consideradas

**Processing time no agregador** (`Instant.now()` no listener). Mais simples e sem dependência do
payload. Recusada: reprocessar o tópico depois de corrigir um bug no agregador mudaria o
resultado, porque cada mensagem cairia numa janela conforme o momento em que fosse relida.
Serve para medir vazão, que não é a pergunta.

**Watermark, agregador com estado sobrevivente a reinício, ou Kafka Streams.** Ficam no desafio
opcional da aula. Não implementados: o custo não se paga para a pergunta atual, e a
consequência de não ter watermark está registrada abaixo.

**`KafkaTemplate<String, Object>` único**, com tópico, `ce_type` e instante como parâmetros.
Evita duplicar a classe de publicação. Recusada: perde a garantia de compilação de que cada
serviço publica exatamente um tipo de evento.

**Decidir a recusa dentro do `VendaService`.** Recusada: mistura a decisão de um sistema
externo com a regra de negócio de reservar, e o serviço deixaria de "reagir" para "decidir".

**Reusar o `eventoId` da reserva na compensação.** Era o que o contrato descrevia antes desta
etapa. Recusada pelo motivo da deduplicação acima; o `contrato.md` foi corrigido.

**Um único tópico para reserva e compensação.** Tecnicamente possível, porque o `ce_type`
diferencia os dois. Recusada para manter os contratos versionáveis de forma independente.

## Consequências aceitas

**Uma janela já lida pode mudar.** Sem watermark, um evento atrasado atualiza uma janela antiga
e quem consultou o endpoint antes vê um total diferente depois. Aceito porque a pergunta é de
relatório, não de decisão.

**O agregador não deduplica.** A soma é acumulativa, então uma reentrega da mesma mensagem
(at-least-once) contaria o ingresso duas vezes. O resultado é reproduzível por reprocessamento
com a tabela zerada, mas não é imune a reentrega. Se isso virar problema, o agregador passa a
registrar o `eventoId` como o consumidor idempotente já faz.

**O agregador mede reservas, não saldo.** Uma compensação não subtrai da agregação: ela responde
"quantos ingressos foram reservados", não "quantos continuam reservados". O saldo real é do
event store e da projeção `disponibilidade_por_setor` (ADR-005 e ADR-007).

**O `reservasAceitas` é volátil.** Some se o `servico-vendas` reiniciar, e uma compensação
depois do reinício vira 404. Num sistema real seria uma tabela de compras do próprio
`servico-vendas`, dono do conceito de compra. O `servico-ingressos` não deve guardar isso: ele só
conhece estoque por setor, e perguntar o conteúdo de uma compra a ele exigiria uma chamada
síncrona entre os serviços.

**A recusa é simulada por chamada manual.** Não há integração com gateway real, o que o ADR-002 já
deixou fora do escopo. Também só cobrimos a recusa: a expiração da reserva, citada no ADR-002
como segundo gatilho, não está implementada.

**O contrato depende de disciplina, não de ferramenta.** Sem schema registry, nada impede um
produtor de mudar o significado de um campo sem mudar o tipo. A proteção é o processo: mudança de
significado é nova versão do tipo do evento.
