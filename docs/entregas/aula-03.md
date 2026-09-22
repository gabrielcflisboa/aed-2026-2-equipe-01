# Entrega — Aula 03

## O que foi feito nesta etapa

- `docs/contrato.md`: contrato da carga de `IngressoReservadoEvent` (campos,
  tipo, obrigatoriedade, significado), formato de data, chave de partição e
  regra de compatibilidade **BACKWARD**.
- Segundo consumidor do mesmo tópico `vendas.ingresso.reservado.v1`, com
  `group.id` próprio (`servico-ingressos-agregador-reservas`), rodando dentro
  do processo `servico-ingressos` ao lado do consumidor idempotente da aula 02
  — os dois processam a mesma mensagem, cada um com seu próprio offset.
- Agregação por janela de tempo de 1 minuto, alinhada por relógio (`:00`,
  `:01`, `:02`...), somando a quantidade de ingressos reservados por
  `(evento, setor)`.
- Resultado observável em `GET /agregacao/reservas-por-setor` (porta `8082`)
  e em log, com partição e offset de cada mensagem processada.

## 1. Qual pergunta de negócio a agregação responde

Quantos ingressos foram reservados por setor, em cada evento de
entretenimento, a cada janela de 1 minuto. É o "relatório de ocupação por
setor/evento" que o [ADR-002](../adr/ADR-002-dominio-do-projeto.md) já havia
antecipado como algo que valeria reprocessar — esta etapa implementa
exatamente essa promessa.

## 2. Qual relógio foi escolhido, e por quê

**Event time**: o campo `reservadoEm` do próprio evento, não o instante em
que a mensagem chegou ao consumidor. A pergunta é sobre o ritmo real de
reservas do negócio; se usássemos processing time, o resultado dependeria de
quando o agregador processou cada mensagem — e não do que de fato aconteceu
na loja de ingressos.

## 3. O que acontece com um evento que chega atrasado

Não há watermark nesta etapa (item do desafio opcional, fora do escopo
pedido). Um evento atrasado apenas atualiza (upsert) a linha da janela à qual
seu `reservadoEm` pertence, mesmo que essa janela já esteja "no passado" em
relação ao relógio de parede — não existe fechamento explícito de janela.
Consequência aceita: quem consulta o endpoint pode ver o total de uma janela
antiga mudar depois de já tê-la lido antes.

## 4. Se o fluxo fosse reprocessado do começo amanhã, o resultado seria o mesmo?

Sim. A janela de cada evento é calculada a partir de `reservadoEm`, um dado
do domínio que viaja dentro do payload — não do instante em que o
agregador leu a mensagem. Reprocessar o tópico do início, com a tabela de
agregação zerada, produz exatamente os mesmos totais por `(evento, setor,
janela)`, porque a única entrada usada no cálculo da janela é um valor que
não muda entre a primeira leitura e um reprocessamento futuro.
