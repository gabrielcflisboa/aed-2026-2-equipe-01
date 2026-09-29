# AGENTS.md — padrões obrigatórios deste repositório

Este arquivo orienta qualquer pessoa (ou agente de IA) que for implementar
código neste repositório. Os padrões abaixo vêm do enunciado da Aula 02 e são
verificados na correção — desvio sem justificativa em ADR custa nota.

## Domínio do projeto

Venda de ingressos para eventos. Ver [docs/adr/ADR-002-dominio-do-projeto.md](docs/adr/ADR-002-dominio-do-projeto.md)
para o fluxo completo, os quatro critérios atendidos e as consequências aceitas.
Não redefina o domínio sem atualizar o ADR primeiro.

## Serviços

- `servico-vendas` — publisher. Recebe a solicitação de compra via HTTP,
  valida limite por CPF e disponibilidade, publica `IngressoReservadoEvent`.
  Hospeda o gateway de pagamento simulado, que publica
  `PagamentoRecusadoEvent`, e consome essa recusa para publicar
  `IngressoLiberadoEvent`. Não persiste nada: reservas e cotas ficam em
  memória. Tem DLQ própria para as recusas que não consegue tratar.
- `servico-ingressos` — consumer idempotente. Mantém o estoque de ingressos
  por setor com event sourcing, aplica o efeito da reserva e devolve o que a
  reserva retirou quando recebe a liberação, com H2 como banco de runtime.
  Tem as DLQs de reservas e de liberações, o endpoint de reprocessamento
  (`/reprocessamentos`) e a consulta do estoque (`/estoque/{evento}/{setor}`).

Os dois são projetos Maven **independentes**: sem POM pai, sem módulo de
contrato compartilhado. Cada lado declara a própria classe do evento.

## Commits semânticos (obrigatório)

Todo commit segue `tipo(escopo): descrição curta no imperativo`, escopo
opcional mas recomendado quando o commit afeta só um serviço (`vendas` ou
`ingressos`).

| Tipo | Quando usar |
|---|---|
| `feat` | novo código de funcionalidade (evento, controller, service, listener...) |
| `fix` | correção de comportamento incorreto |
| `docs` | ADR, README, IA.md, folha de rosto, comentários de documentação |
| `chore` | configuração de repositório: `.gitignore`, `docker-compose.yml`, scaffold do Initializr |
| `test` | testes automatizados |
| `refactor` | mudança de estrutura sem alterar comportamento |

Exemplos: `docs(adr): registra ADR-002 — domínio de venda de ingressos`,
`feat(vendas): implementa VendaController com resposta 202`,
`test(ingressos): idempotência — mesmo evento entregue 3x`.

A ordem de commit importa: o ADR-002 entra ANTES de qualquer `.java` (ver
checklist do enunciado, item 19 — `git log --reverse` é conferido).

## Pacotes e nomenclatura (obrigatório, verificado na correção)

| Regra | Certo | Errado |
|---|---|---|
| Raiz do nome em português, sufixo em inglês | `IngressoReservadoEvent`, `ItemDoIngressoVO` | `IngressoReservadoEvento`, `TicketReservedEvent` |
| Quatro pacotes, com estes nomes | (raiz), `controller`, `domain`, `service` | `events`, `entities`, `vos`, `utils`, `impl` |
| Sufixos da lista fechada | `Application`, `Config`, `Controller`, `Listener`, `Service`, `Repository`, `Event`, `VO` | `Util`, `Helper`, `Manager`, `Impl`, `DTO`, `Producer` |
| O nome descreve o papel, não o fornecedor | `VendaService`, `clienteDoBroker` | `VendaKafkaPublisher`, `kafkaTemplate` |
| `domain` não conhece framework de infraestrutura | `domain` só importa biblioteca padrão e anotações de serialização | `Event` importando `org.apache.kafka` |
| `@Transactional` só no `Service` | transação começa e termina no serviço de aplicação | `@Transactional` no `Listener` ou no `Controller` |

Pacote raiz: `br.pucminas.aed.vendas` e `br.pucminas.aed.ingressos` — sem
underscore, sem repetir o prefixo "servico" dentro do nome do pacote.

Desvio registrado da lista de sufixos: exceções usam o sufixo `Exception`,
por convenção do Java (`CompraNaoEncontradaException`,
`ReservaAindaNaoProcessadaException`). É o único sufixo fora da lista.

## O evento

- Nome no particípio, descrevendo um fato ocorrido, nunca um comando
  (`IngressoReservado`, não `ReservarIngresso`).
- Classe imutável explícita: campos `private final`, sem setter, cópia
  defensiva de coleção. Não usar `record` — os mecanismos precisam ficar
  à vista.
- Identidade própria: um `eventoId`, distinto do id da entidade de negócio.
  A chave de deduplicação é o `eventoId`, nunca o id da entidade.
- Datas em ISO-8601, nunca epoch.

## O publisher (`servico-vendas`)

- Envelope CloudEvents 1.0 em modo binário: `ce_specversion`, `ce_id`,
  `ce_source`, `ce_type` nos cabeçalhos, mais `ce_time`.
- `type` numa grafia só, versionada: `dominio.entidade.fato.v1`.
- Publicar com chave de partição — a menor unidade cuja ordem o negócio exige.
- O retorno do `send()` tem dono: tratado numa classe nomeada
  (`VendaCallbackService`), nunca ignorado.
- A API HTTP responde 202, não 200, quando o efeito ainda não aconteceu.

## O consumidor idempotente (`servico-ingressos`)

- Recebe o mesmo evento mais de uma vez e produz efeito UMA vez.
- Memória do que já foi processado: tabela com o `eventoId` como chave primária.
- O efeito de negócio e o registro da deduplicação no MESMO commit.
- A confirmação do offset (`ack.acknowledge()`) vem DEPOIS do commit — é o
  que caracteriza at-least-once.
- Declarar menos campos do que o publisher publica, de propósito — campos
  desconhecidos devem ser ignorados (consumidor tolerante).
- Teste automatizado que entregue o mesmo evento três vezes e verifique o
  efeito único.

## O caminho de falha

As decisões estão no [ADR-006](docs/adr/ADR-006-resiliencia.md).

- Listener sem `try/catch`. A exceção precisa chegar ao tratador de erro do
  `ResilienciaConfig`; um `catch` avança o offset e o evento some.
- A lista de falhas permanentes mora no `ResilienciaConfig` de cada serviço.
  O que não está nela é transitório: retenta até quatro vezes, com espera de
  1, 2, 4 e 8 segundos, e depois vai para a DLQ.
- Carga inválida vira falha permanente no construtor `@JsonCreator` do evento
  consumido, com `Objects.requireNonNull` e validações de regra.
- DLQ com o nome do tópico original acrescido de `.dlq`, declarada como
  `NewTopic` pelo serviço que escreve nela.
- Reprocessamento só por comando explícito, pelo `ce_id`, em
  `POST /reprocessamentos`. Nunca um consumidor que republica a DLQ sozinho.
- Todo consumidor de um tópico com DLQ é idempotente, porque reprocessar
  entrega o evento de novo a todos os grupos do tópico.
- Compensação é evento novo, no particípio, nunca `UPDATE` nem `DELETE` no log
  do estoque.

## Estrutura de diretórios obrigatória

```
aed-2026-2-equipe-01/
├── README.md
├── docs/
│   ├── adr/ADR-002-dominio-do-projeto.md
│   ├── adr/ADR-003-chave-de-particao.md
│   ├── adr/ADR-004-contrato-agregador-e-compensacao.md
│   ├── adr/ADR-005-event-sourcing.md
│   ├── adr/ADR-006-resiliencia.md
│   ├── adr/ADR-007-projecoes-e-replay.md
│   ├── entregas/aula-02.md
│   ├── arquitetura.md
│   ├── contrato.md
│   ├── apresentacao.pdf
│   └── IA.md
├── servico-vendas/
└── servico-ingressos/
```

Caminho fora do padrão não é encontrado na correção. Não criar pastas
adicionais fora deste layout sem justificar em ADR.

## Ordem de criação (não altera nota, mas é conferida)

1. `README.md`
2. `docs/adr/ADR-002-dominio-do-projeto.md` — ANTES da primeira linha de código
3. `docs/IA.md` — ao longo da semana
4. `docs/entregas/aula-02.md` — por último

## Checklist rápido antes de commitar

- Nome de evento não tem raiz CRUD (`create`, `update`, `delete`, `salvar`).
- Nenhuma data em epoch no corpo da mensagem.
- Os quatro cabeçalhos `ce_*` obrigatórios estão presentes.
- `domain/` não importa `org.apache.kafka` nem `org.springframework` de infraestrutura.
- `@Transactional` não aparece em `controller/`.
- Nenhum `try/catch` em listener.
- Consumidor novo nasce idempotente, e a falha permanente dele vai para a DLQ.
- Nenhum dado pessoal real (CPF, e-mail, telefone) em código ou cargas de exemplo.
