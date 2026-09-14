# ADR-006: projeções e reconstrução do estado de leitura da disponibilidade

## Status

Aceita · 2026-09-08 · Equipe 01

Sucede o [ADR-005](ADR-005-event-sourcing.md), que estabeleceu o Event Sourcing como fonte da verdade no
`servico-ingressos`. Este ADR decide como o estado de leitura da disponibilidade de ingressos é projetado e reconstruído
de forma assíncrona a partir do log.

## Contexto

O ADR-005 garantiu consistência forte para gravações no *Event Store* (`evento_do_estoque`), rebatendo a decisão de
aceitar ou recusar reservas na reconstrução do agregado `EstoqueDoSetor`. No entanto, como registrado nas consequências
do ADR-005, a consulta direta ao log de eventos para alimentar telas de ‘vitrine’ e consultas de saldo traz custos
proibitivos de I/O a cada requisição de leitura.

Surgiu a necessidade de disponibilizar uma visão de leitura otimizada (`disponibilidade_por_setor`) que atenda às telas
com respostas de baixa latência, sem impactar a escrita ou violar a premissa de que a decisão de negócio continua a ler
unicamente o stream original.

## Decisão

Adotar o padrão **CQRS (Command Query Responsibility Segregation)** para separar a escrita (Event Store) da leitura
(Projeção). A tabela de leitura `disponibilidade_por_setor` é tratada estritamente como um estado derivado e
descartável.

### Mapeamento e Atualização Incremental

A atualização da projeção é feita por um serviço dedicado (`DisponibilidadeProjecaoService`), que consome os fatos do
log em lotes e os traduz em atualizações diretas na tabela de consulta:

- `SetorAbertoEvent` → Cria o registro inicial com capacidade e saldo disponível.
- `IngressoRetiradoEvent` → Decrementa a quantidade disponível e incrementa a contagem de retirados.
- `IngressoDevolvidoEvent` → Incrementa a quantidade disponível e decrementa a contagem de retirados (compensação).
- `ReservaRecusadaEvent` → Ignorado para o saldo de disponibilidade (evento sem impacto direto no inventário ativo,
  podendo alimentar projeções de demanda no futuro).

### Controle de Leitura por Checkpoint

O estado do consumo do log é mantido na tabela `projecao_checkpoint`. Cada projeção registrada possui um ponteiro com a
última `sequencia` processada.

A sincronização ocorre em background via polling agendado (`@Scheduled` no `ReconstrucaoService`), processando novos
lotes de eventos incrementalmente com frequência definida pela propriedade `app.projecoes.intervalo`.

### Replay e Reconstrução Determinística

A projeção não é fonte da verdade. O método `reconstruir()` do `ReconstrucaoService` permite zerar integralmente a
tabela `disponibilidade_por_setor` e a tabela `projecao_checkpoint`, reprocessando todo o *Event Store* a partir da
sequência zero.

A reconstrução é reproduzível e idempotente: reexecutá-la N vezes garante exatamente a mesma fotografia de leitura ao
final do processamento.

## Defasagem Tolerada

| Leitura                         | Quem faz                      | Lê de                                      | Defasagem tolerada                       | Por que essa tolerância                                                                                                                                                              |
|---------------------------------|-------------------------------|--------------------------------------------|------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Aceitar ou recusar reserva**  | `IngressoService`             | **Event Store** (`evento_do_estoque`)      | **Zero**                                 | Leitura transacional crítica que gera fatos. O agregado é reconstruído do stream e a versão previne *overselling*.                                                                   |
| **Consulta de saldo por setor** | Telas de vitrine / Dashboards | **Projeção** (`disponibilidade_por_setor`) | **Eventual** (`app.projecoes.intervalo`) | Consultas de interface toleram pequenos atrasos de sincronização (ex: 1s). Um valor levemente defasado na vitrine é corrigido no momento em que a reserva tenta ser efetuada no log. |

## Alternativas Consideradas

**Projeção síncrona (na mesma transação do evento).** Atualizar a tabela de leitura na mesma transação que grava o
evento no log. Recusada porque acoplava o desempenho do processador de eventos à escrita na projeção e transformava a
tabela de leitura em um gargalo de concorrência (*lock* de linhas durante a gravação do fato).

**Triggers de banco de dados.** Delegar ao H2 a atualização automática da tabela de saldo através de triggers na tabela
`evento_do_estoque`. Recusada por acoplar a regra da projeção ao dialeto de banco de dados e esconder a lógica de
reconstrução de estado fora do código Java do projeto.

## Consequências Aceitas

**Consistência eventual nas leituras de vitrine.** Existe uma janela de tempo entre a gravação do evento no log e a
atualização da tabela de projeção determinada por `app.projecoes.intervalo`. Aceitamos porque a tentativa de reserva
real sempre valida a capacidade contra o log imutável.

**Escritas externas indevidas na tabela de projeção são sobrescritas no replay.** Caso uma manutenção manual altere
dados da tabela `disponibilidade_por_setor`, esses dados não sobrevivem a uma reconstrução da projeção, garantindo a
recuperação da consistência.

**A tabela de checkpoint passa a ser vital para a performance de inicialização.** Se o checkpoint for perdido, a
projeção precisa ser reconstruída do início. O custo da reconstrução total cresce linearmente com o tamanho do log de
eventos.