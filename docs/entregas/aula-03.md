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
- Atualização da branch: caminho de compensação por pagamento recusado ligado ao
  Kafka (seção 5) e `docs/contrato.md` corrigido e completo para o evento de compensação.

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

Isso é verificado por teste: `reprocessarDoComecoComATabelaZeradaDaOsMesmosTotais` processa
as reservas, zera a tabela, processa de novo em ordem inversa e compara os totais. Sem zerar
a tabela o total dobraria, porque o agregador não deduplica (ver ADR-004).

## 5. Atualização da branch: compensação por pagamento recusado

O `IngressoReservaCompensadaEvent` já existia, mas nada o publicava nem consumia. Agora o
ciclo fecha em dois fluxos independentes:

1. **Reserva:** `POST /vendas/reservas` publica `IngressoReservadoEvent` e o
   `servico-ingressos` debita o estoque.
2. **Recusa:** `POST /vendas/reservas/{compraId}/compensacoes` simula o webhook de um
   gateway externo. O `GatewayDePagamentoService` decide o motivo da recusa, o
   `VendaService` reage (busca a reserva, publica a compensação no tópico
   `vendas.ingresso.reserva-compensada.v1`) e o `servico-ingressos` devolve o estoque.

Decisões: um `KafkaTemplate` tipado por evento (sem template genérico); o `eventoId` da
compensação é sempre novo, nunca o da reserva, porque a deduplicação usa esse id como
chave e descartaria a devolução como "já processada"; a deduplicação é feita uma vez por
mensagem (`processarCompensacao`). Compensar a mesma compra duas vezes devolve 404.
O `reservasAceitas` em memória é uma simplificação da simulação (num sistema real seria
uma tabela de compras do `servico-vendas`).

Verificado com Kafka real: após reserva e recusa, `IngressoRetirado` e `IngressoDevolvido`
ficam no log e `disponibilidade_por_setor` volta a `retirados=0`. Testes: 13 no
`servico-vendas` e 26 no `servico-ingressos`.

## Onde está cada coisa

- Decisões desta etapa: [ADR-004](../adr/ADR-004-contrato-agregador-e-compensacao.md)
- Contrato dos eventos (campos, significado, chave de partição, BACKWARD): [docs/contrato.md](../contrato.md)
- Padrões de pacote/nomenclatura: [AGENTS.md](../../AGENTS.md)
- Registro de uso de IA: [docs/IA.md](../IA.md)
- Agregador, em `servico-ingressos`:
  - [AgregadorDeReservasListener.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/controller/AgregadorDeReservasListener.java), o segundo consumidor, com `group.id` próprio
  - [AgregacaoDeReservasService.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/AgregacaoDeReservasService.java), o cálculo da janela por `reservadoEm`
  - [AgregacaoJdbcRepository.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/AgregacaoJdbcRepository.java), o upsert por (evento, setor, janela)
  - [AgregacaoController.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/controller/AgregacaoController.java), o endpoint que torna o resultado observável
- Compensação, em `servico-vendas`:
  - [GatewayDePagamentoService.java](../../servico-vendas/src/main/java/br/pucminas/aed/vendas/service/GatewayDePagamentoService.java), o gateway simulado
  - [VendaService.java](../../servico-vendas/src/main/java/br/pucminas/aed/vendas/service/VendaService.java) e [VendaController.java](../../servico-vendas/src/main/java/br/pucminas/aed/vendas/controller/VendaController.java)
  - [VendaCompensacaoCallbackService.java](../../servico-vendas/src/main/java/br/pucminas/aed/vendas/service/VendaCompensacaoCallbackService.java), a publicação no Kafka
- Compensação, em `servico-ingressos`: [IngressoListener.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/controller/IngressoListener.java) e `processarCompensacao` em [IngressoService.java](../../servico-ingressos/src/main/java/br/pucminas/aed/ingressos/service/IngressoService.java)
- Testes:
  - [AgregacaoDeReservasServiceTest.java](../../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/AgregacaoDeReservasServiceTest.java), janela, evento atrasado e reprocessamento
  - [IngressoServiceIdempotenciaTest.java](../../servico-ingressos/src/test/java/br/pucminas/aed/ingressos/service/IngressoServiceIdempotenciaTest.java), mesma compensação 3x, efeito único
  - [VendaServiceTest.java](../../servico-vendas/src/test/java/br/pucminas/aed/vendas/service/VendaServiceTest.java) e [VendaCompensacaoCallbackServiceTest.java](../../servico-vendas/src/test/java/br/pucminas/aed/vendas/service/VendaCompensacaoCallbackServiceTest.java)

## Por onde começar a leitura

1. [ADR-004](../adr/ADR-004-contrato-agregador-e-compensacao.md), para as três decisões e o que custaram.
2. [docs/contrato.md](../contrato.md), para o significado de cada campo e a regra BACKWARD.
3. `AgregacaoDeReservasService`, para ver a janela calculada a partir de `reservadoEm` e nunca da hora de chegada.
4. `AgregacaoDeReservasServiceTest`, para ver janela, evento atrasado e reprocessamento rodando.
5. `VendaService.compensarPagamentoRecusado` e `IngressoService.processarCompensacao`, para o caminho de compensação nas duas pontas.

## Como rodar

Ver [README.md](../../README.md#como-rodar-máquina-limpa) — resumo:

```powershell
docker compose up -d
cd servico-ingressos; ./mvnw.cmd spring-boot:run
cd servico-vendas; ./mvnw.cmd spring-boot:run
curl -X POST http://localhost:8080/vendas/reservas -H "Content-Type: application/json" -d '@exemplo-reserva.json'
Invoke-RestMethod "http://localhost:8082/agregacao/reservas-por-setor?evento=show-pucminas-2026"
curl -X POST http://localhost:8080/vendas/reservas/compra-0001/compensacoes
```

O agregador e o consumidor idempotente sobem no mesmo processo, com `group.id` diferentes, e os dois
processam cada reserva. Testes: `./mvnw.cmd test` em cada serviço.

## Quem fez o quê

| Integrante                         | O que fez |
|------------------------------------|---|
| Gabriel Campos Ferreira Lisboa     | Agregador por janela de tempo (`AgregadorDeReservasListener`, `AgregacaoDeReservasService`, `AgregacaoJdbcRepository`, `AgregacaoController`), `reservadoEm` no evento consumido, testes do agregador, escolha do relógio e esta folha de entrega, registro em `IA.md`. |
| Maria Luísa Lacerda                | `docs/contrato.md`: documentação do contrato do evento `IngressoReservadoEvent`. |
| Amir Gabriel Dantas Santos Andrade | Atualização da branch: caminho de compensação por pagamento recusado nas duas pontas (`GatewayDePagamentoService`, `VendaCompensacaoCallbackService`, endpoint de compensação, `processarCompensacao`, novo listener), correção e ampliação do contrato do evento de compensação, teste de reprocessamento do agregador, ADR-004 e a seção 5 e os tópicos de leitura desta folha. |

Os demais integrantes da equipe não têm commit registrado nesta etapa.
