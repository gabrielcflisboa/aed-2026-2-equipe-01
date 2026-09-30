# Entrega — Aula 04

## O que foi agregado, com qual janela, e por que essa janela

O que já existia da aula 03 continua sendo a agregação desta etapa: o
`AgregacaoDeReservasService` soma a quantidade de ingressos reservados por `(evento, setor)`,
alimentado pelo `AgregadorDeReservasListener` (segundo consumidor, `group.id` próprio, mesmo
tópico de reservas).

**A janela é tumbling, de 1 minuto, alinhada por relógio** (`:00`, `:01`, `:02`...), e não por
quando o processo do agregador subiu — o início de cada janela é o piso do `reservadoEm` em
múltiplos de 60 segundos (`AgregacaoDeReservasService.janelaDe`).

Por que tumbling, e não as outras três da aula:

- **Não hopping.** A pergunta é "quantos ingressos foram reservados nesse minuto", um total
  de período, não uma média móvel. Com hopping, o mesmo ingresso entraria em mais de uma
  janela — exatamente o problema do slide da aula (sete pedidos, R$ 2.634,00 reais de
  faturamento, e o relatório hopping somando R$ 13.170,00, cinco vezes o valor real, sem
  nenhum erro ou aviso). Somar as janelas tumbling do nosso agregador dá o total exato de
  ingressos reservados no evento; somar janelas hopping daria um número maior que o real,
  plausível e errado.
- **Não sliding.** Uma janela por evento (a cada reserva) responde "quantas reservas
  ocorreram nos últimos 60 segundos antes desta", uma pergunta sobre o instante de cada
  reserva, não sobre um relatório por período — não é a pergunta que o `GET
  /agregacao/reservas-por-setor` faz.
- **Não session.** Session agrupa por dono, sem tamanho fixo, fechando no silêncio (o que
  um cliente fez numa mesma visita). O agregador não pergunta por comprador — pergunta por
  `(evento, setor)`, uma dimensão que já tem tamanho de janela natural (a taxa de venda por
  minuto), não um período de silêncio entre reservas.

A chave de partição do tópico que o agregador lê (`evento`) é a mesma dimensão pela qual ele
agrega, então essa soma não precisa de repartition topic — a justificativa completa está no
[ADR-003](../adr/ADR-003-chave-de-particao.md).

## Quem fez o quê

| Integrante | O que fez |
|---|---|
| Amir Gabriel Dantas Santos Andrade | ADR-003 (chave de partição) e esta folha de entrega, revendo a janela e a chave já escolhidas nas aulas 02, 03 e 05 sob o critério da aula 04; registro em `docs/IA.md`. |
