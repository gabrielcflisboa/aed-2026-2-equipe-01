# ADR-002 — Domínio do projeto

## Status
Aceita · 2026-08-16 · Equipe 02

## Contexto
A equipe precisa de um processo de negócio real para servir de base aos
próximos cinco encontros da disciplina. Escolhemos a venda de ingressos para
eventos (shows, jogos, peças), trazida por Gabriel Campos Ferreira Lisboa, que teve
contato com o problema durante a liberação da compra de ingressos de um show, o qual ele 
estava interessado. O domínio interessa porque o intervalo entre reservar um 
lugar e confirmar o pagamento é onde a maior parte dos 
sistemas de venda de ingresso falha sob concorrência: dois
compradores podem disputar o mesmo assento, e o pagamento pode ser recusado
depois que o ingresso já foi retirado do estoque.

## Decisão
Um comprador solicita a reserva de um ou mais ingressos de um setor/evento;
o serviço de vendas valida limite por CPF e disponibilidade do setor, publica
um fato de reserva e aciona um gateway de pagamento externo (simulado); se o
pagamento for recusado ou a reserva expirar sem confirmação, o ingresso volta
para o estoque do setor por meio de um evento de compensação.

Como o domínio atende os quatro critérios:
 - ponto de decisao com regra de negocio: limite de ingressos por CPF e
   verificação de capacidade do setor antes de aceitar a reserva
 - sistema externo: gateway de pagamento (simulado) que autoriza ou recusa a cobrança
 - caminho de excecao com compensacao: reserva expirada ou pagamento recusado
   libera o ingresso de volta ao estoque do setor
 - algo que valha reprocessar: relatório de ocupação por setor/evento e
   auditoria de tentativas de reserva acima do limite por CPF

## Alternativas consideradas
- Pedido → estoque genérico (e-commerce): recusado por ser exatamente o
  recorte que a demonstração da aula 01 já cobre.
- Cadastro de clientes/CRUD de eventos: recusado por não ter caminho de
  exceção nem nada que valha reprocessar — travaria na aula 05 (Saga/Event
  Sourcing).

## Consequencias aceitas
Vamos modelar reserva e confirmação como fatos separados, o que exige
controlar um estado intermediário (reservado, mas não pago) — isso adiciona
complexidade que um fluxo síncrono simples não teria. Ficam fora do escopo
desta etapa: emissão real de QR code/e-ticket, integração com gateway de
pagamento real, e cancelamento pelo próprio comprador (só cobrimos expiração
e recusa). Na aula 05, a Saga de compensação (liberar assento) vai exigir
cuidado para não reprocessar uma reserva já expirada duas vezes — é algo que
antecipamos aqui e vamos tratar via idempotência do consumidor.