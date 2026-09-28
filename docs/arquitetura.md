# Arquitetura do sistema de venda de ingressos

## 1. O domínio

O sistema trata da reserva de ingressos para shows organizados em setores de capacidade fixa. Cada setor mantém sua própria disponibilidade e constitui uma fronteira independente para a decisão de retirar ou devolver ingressos.

Na configuração atual, o evento `show-pucminas-2026` possui três setores:

| Setor | Capacidade |
|---|---:|
| PISTA | 100 |
| CAMAROTE | 20 |
| ARQUIBANCADA | 50 |

A reserva está sujeita a duas regras principais. Um CPF pode reservar no máximo quatro ingressos, e uma reserva só pode retirar ingressos de um setor quando existe disponibilidade suficiente. A capacidade de cada setor é registrada no próprio histórico do estoque e não como um saldo inicial inserido diretamente em uma tabela.

Reserva e pagamento são momentos distintos. Depois que uma reserva é aceita, os ingressos já não estão disponíveis para outro comprador, embora o pagamento ainda possa ser recusado. Existe, portanto, um estado intermediário legítimo no qual os ingressos estão reservados, mas a compra ainda não está paga.

O pagamento representa a interação com uma operadora externa. Neste sistema essa operadora é simulada pelo gateway que roda junto ao `servico-vendas`. Uma recusa é iniciada por `POST /pagamentos/{compraId}/recusas`, mas a consequência da recusa não é executada como uma chamada direta ao serviço de estoque. A recusa é publicada como fato e inicia uma Saga coreografada que termina com a devolução dos ingressos retirados pela reserva.

O estoque é mantido por setor e pode explicar seu estado atual por meio do histórico de fatos. Além da disponibilidade, o sistema mantém uma agregação das reservas por evento e setor em janelas de um minuto. Assim, é possível responder quantos ingressos restam em determinado setor, quais fatos levaram a esse estado e quantos ingressos foram reservados em cada janela da agregação.

O escopo termina antes das etapas posteriores de uma operação comercial completa. O sistema não emite o ingresso ao comprador, não realiza cobrança financeira real, não executa reembolso e não controla a entrada no local do evento. O gateway existente representa apenas a fronteira necessária para simular a resposta externa de pagamento.

## 2. Os eventos

A comunicação entre os componentes usa fatos de domínio. Os nomes dos eventos descrevem acontecimentos que já ocorreram, em vez de ordens para que outro componente execute determinada ação. Por isso são expressos no particípio: um ingresso foi reservado ou liberado e um pagamento foi recusado.

Os contratos completos dos eventos publicados estão em [`docs/contrato.md`](contrato.md).

### 2.1 Eventos publicados

Três fatos participam do fluxo principal:

| Evento | Tipo/tópico | Significado |
|---|---|---|
| `IngressoReservadoEvent` | `vendas.ingresso.reservado.v1` | Uma reserva foi aceita pelo `servico-vendas`. |
| `PagamentoRecusadoEvent` | `pagamentos.pagamento.recusado.v1` | O gateway simulado recusou o pagamento de uma compra. |
| `IngressoLiberadoEvent` | `vendas.ingresso.liberado.v1` | O `servico-vendas` decidiu liberar os ingressos retirados pela reserva após a recusa do pagamento. |

`IngressoReservadoEvent` é um fato e não um pedido para baixar estoque. O `servico-ingressos` decide, ao consumi-lo e reconstruir o agregado do setor, se aquela reserva efetivamente consegue retirar ingressos.

`PagamentoRecusadoEvent` representa a resposta do sistema externo de pagamento. O fato é publicado pelo gateway simulado e consumido pelo próprio `servico-vendas`, em outro ponto do fluxo. Isso mantém separadas a ocorrência da recusa e a reação da venda à recusa.

`IngressoLiberadoEvent` representa a decisão de desfazer o efeito da reserva sobre o estoque. O nome substitui a antiga ideia de "reserva compensada": `liberado` descreve o fato de domínio observado pelos consumidores, enquanto "compensação" descrevia o mecanismo usado para chegar a esse resultado.

### 2.2 Eventos internos do estoque

O `servico-ingressos` mantém quatro tipos de fatos no event store:

| Evento | Significado |
|---|---|
| `SetorAbertoEvent` | Registra a capacidade inicial de um setor. |
| `IngressoRetiradoEvent` | Registra quantos ingressos uma reserva efetivamente retirou daquele setor. |
| `ReservaRecusadaEvent` | Registra que uma tentativa de reserva não pôde retirar ingressos. |
| `IngressoDevolvidoEvent` | Registra a devolução ao setor dos ingressos anteriormente retirados por uma reserva. |

Esses eventos pertencem ao histórico interno do estoque e não são publicados como parte do contrato entre os serviços. O estado de `EstoqueDoSetor` é reconstruído aplicando esses fatos na ordem do stream.

A devolução não apaga um `IngressoRetiradoEvent`. O histórico preserva tanto a retirada quanto a devolução posterior. Assim, o estado atual pode mudar sem apagar o fato de que a reserva existiu.

### 2.3 Identidade e correlação

Cada evento possui seu próprio `eventoId`. Essa identidade é usada para deduplicação e permite reconhecer uma nova entrega do mesmo fato.

Além da identidade do próprio evento, os fatos da Saga mantêm referências explícitas aos acontecimentos anteriores. `reservaEventoId` identifica o `IngressoReservadoEvent` cuja retirada precisa ser desfeita. `pagamentoEventoId` identifica o `PagamentoRecusadoEvent` que originou a liberação.

A relação pode ser lida como uma cadeia:

`IngressoReservadoEvent.eventoId`
→ `IngressoLiberadoEvent.reservaEventoId`

`PagamentoRecusadoEvent.eventoId`
→ `IngressoLiberadoEvent.pagamentoEventoId`

Essa correlação é especialmente importante no estoque. O `compraId` serve para correlacionar a operação comercial e os logs, mas o event store do estoque conhece a reserva pelo fato que produziu a retirada. Por isso a liberação identifica explicitamente `reservaEventoId`.

O estoque não devolve cegamente `itens[].quantidade` recebido na liberação. Ele localiza no próprio histórico quanto aquela reserva efetivamente retirou e devolve essa quantidade. A informação recebida no evento não substitui o log como fonte da verdade.

### 2.4 Envelope e evolução

Os eventos publicados seguem CloudEvents 1.0 em modo binário. Os atributos CloudEvents são transportados nos cabeçalhos da mensagem, enquanto o `data` é serializado como JSON. As datas são representadas em ISO-8601.

A compatibilidade adotada é BACKWARD. Uma versão nova do produtor deve continuar podendo ser consumida pelo consumidor existente dentro da evolução prevista pelo contrato. O contexto operacional permite implantação coordenada porque uma única equipe controla os dois lados da integração. Quando uma mudança exige atualização dos dois componentes, o consumidor é implantado primeiro e o produtor depois.

Mudanças incompatíveis de significado ou de campos obrigatórios não são escondidas dentro do mesmo contrato. A substituição de `vendas.ingresso.reserva-compensada.v1` por `vendas.ingresso.liberado.v1` é um exemplo: a mudança introduz uma nova semântica e novas referências necessárias para correlacionar corretamente os fatos.

## 3. O desenho

A solução é dividida em dois serviços de aplicação conectados pelos tópicos de eventos.

O `servico-vendas`, na porta 8080, recebe reservas, mantém em memória as informações necessárias às regras de venda e simula a integração com o gateway de pagamento. Ele continua sem banco próprio. Além de publicar reservas e liberações, passa a consumir as recusas de pagamento no grupo `servico-vendas`.

O `servico-ingressos`, na porta 8082, mantém o estoque usando event sourcing. Seu estado persistente fica em H2, incluindo o log `evento_do_estoque` e os registros usados para deduplicação. O mesmo processo também executa o agregador de reservas, porém em um grupo Kafka independente do consumidor responsável pelo estoque.

O fluxo principal é:

```mermaid
flowchart LR
    comprador(["Comprador"])
    recusa(["Recusa simulada do pagamento"])
    operacao(["Operação"])

    subgraph vendas["servico-vendas :8080"]
        vendaController["VendaController<br/>POST /vendas/reservas"]
        gateway["Gateway de pagamento simulado<br/>POST /pagamentos/{compraId}/recusas"]
        pagamentoListener["PagamentoListener<br/>grupo servico-vendas"]
    end

    subgraph kafka["Kafka"]
        reservado[["vendas.ingresso.reservado.v1<br/>chave evento"]]
        recusado[["pagamentos.pagamento.recusado.v1<br/>chave compraId"]]
        liberado[["vendas.ingresso.liberado.v1<br/>chave evento"]]
        dlqReservado[["vendas.ingresso.reservado.v1.dlq"]]
        dlqRecusado[["pagamentos.pagamento.recusado.v1.dlq"]]
        dlqLiberado[["vendas.ingresso.liberado.v1.dlq"]]
    end

    subgraph ingressos["servico-ingressos :8082"]
        ingressoListener["IngressoListener<br/>grupo servico-ingressos"]
        agregador["AgregadorDeReservasListener<br/>grupo servico-ingressos-agregador-reservas"]
        estoque[("H2<br/>evento_do_estoque<br/>evento_processado")]
        reprocessamento["ReprocessamentoController<br/>/reprocessamentos"]
    end

    comprador --> vendaController --> reservado
    recusa --> gateway --> recusado
    recusado --> pagamentoListener --> liberado
    reservado --> ingressoListener
    liberado --> ingressoListener
    reservado --> agregador
    ingressoListener --> estoque
    ingressoListener -.->|"permanente, ou 5 entregas sem sucesso"| dlqReservado
    ingressoListener -.->|"permanente, ou 5 entregas sem sucesso"| dlqLiberado
    agregador -.->|"permanente, ou 5 entregas sem sucesso"| dlqReservado
    pagamentoListener -.->|"permanente, ou 5 entregas sem sucesso"| dlqRecusado
    operacao --> reprocessamento
    reprocessamento -.->|"lê a DLQ e republica no tópico de origem"| reservado
```

### 3.1 Tópicos

| Tópico | Chave | Quem publica | Quem consome, e o grupo | Retenção |
|---|---|---|---|---|
| `vendas.ingresso.reservado.v1` | `evento` | servico-vendas | servico-ingressos (`servico-ingressos`) e agregador (`servico-ingressos-agregador-reservas`) | 7 dias |
| `pagamentos.pagamento.recusado.v1` | `compraId` | gateway simulado, no servico-vendas | servico-vendas (`servico-vendas`) | 7 dias |
| `vendas.ingresso.liberado.v1` | `evento` | servico-vendas | servico-ingressos (`servico-ingressos`) | 7 dias |
| `vendas.ingresso.reservado.v1.dlq` | a original | servico-ingressos, pelos dois grupos | leitura sob demanda | 30 dias |
| `vendas.ingresso.liberado.v1.dlq` | a original | servico-ingressos | leitura sob demanda | 30 dias |
| `pagamentos.pagamento.recusado.v1.dlq` | a original | servico-vendas | leitura sob demanda | 30 dias |

Todos os tópicos possuem atualmente uma partição. Cada DLQ é declarada como `NewTopic` pelo serviço que escreve nela.

A chave `evento` é usada para reserva e liberação porque mantém os fatos de um mesmo evento de entretenimento na mesma partição. Isso preserva a ordem dentro dessa chave. A recusa de pagamento usa `compraId`, pois é a compra que identifica o fluxo tratado pelo `servico-vendas`.

### 3.2 Saga do pagamento recusado

A Saga é coreografada e acontece em três passos.

Primeiro, o `servico-vendas` publica `IngressoReservadoEvent`. O `servico-ingressos` consome esse fato e registra no log do estoque o resultado da tentativa de reserva.

Depois, quando `POST /pagamentos/{compraId}/recusas` é acionado, o gateway simulado publica `PagamentoRecusadoEvent`.

Por fim, o `servico-vendas` consome a recusa, localiza a reserva correspondente, devolve a cota do CPF e publica `IngressoLiberadoEvent`. O `servico-ingressos` consome a liberação, encontra pelo `reservaEventoId` o que aquela reserva realmente retirou e registra um `IngressoDevolvidoEvent`.

Não existe um coordenador central mantendo o estado completo da Saga. Cada participante reage ao fato que lhe interessa e publica o próximo fato quando sua decisão local é concluída.

### 3.3 Semântica de entrega

O processamento assume semântica de pelo menos uma vez. Uma mensagem pode ser entregue novamente quando o processamento anterior não terminou com confirmação do offset.

O reconhecimento ocorre depois da conclusão do processamento que precisa ser preservado. No `servico-ingressos`, isso significa que a confirmação não deve anteceder o commit da alteração persistente. Se houver falha antes desse ponto, a mensagem pode voltar a ser entregue.

Por isso a correção não depende de uma hipótese de entrega exatamente uma vez. Os consumidores são idempotentes e identificam eventos já processados. No estoque, a identidade do evento protege contra repetição da mesma mensagem e a identidade da reserva protege a operação de liberação contra um segundo efeito sobre a mesma retirada.

## 4. As decisões

As decisões arquiteturais que explicam o desenho atual estão registradas nos ADRs do repositório. A tabela resume a decisão central e uma consequência aceita de cada uma.

| ADR | Decisão | Consequência aceita |
|---|---|---|
| [ADR-002](adr/ADR-002-dominio-do-projeto.md) | domínio de venda de ingressos | reserva e pagamento são fatos separados, e o sistema convive com um estado intermediário, reservado e não pago |
| [ADR-003](adr/ADR-003-contrato-agregador-e-compensacao.md) | contrato, agregador por janela e compensação | sem watermark, uma janela da agregação já consultada pode mudar quando chega uma reserva atrasada |
| [ADR-005](adr/ADR-005-event-sourcing.md) | event sourcing no estoque | cada decisão relê o stream do setor, e o log só cresce, sem expurgo nem snapshot |
| [ADR-006](adr/ADR-006-resiliencia.md) | resiliência e Saga | uma falha transitória segura a partição por até 15 s, e com chave `evento` e 1 partição as vendas de todos os shows param juntas |
| [ADR-007](adr/ADR-007-projecoes-e-replay.md) | projeções e replay | a disponibilidade exibida pode estar até 1 s atrás do log |

O ADR-002 estabelece a razão para separar reserva, pagamento e compensação: existe uma interação externa entre a retirada inicial e a confirmação financeira, e essa interação pode falhar.

O ADR-003 estabelece o contrato dos eventos e a agregação temporal. A agregação usa o instante do próprio evento e não o instante em que a mensagem chegou ao consumidor, permitindo reconstrução determinística a partir do mesmo histórico.

O ADR-005 define `EstoqueDoSetor` como agregado e o log append-only como fonte da verdade. A capacidade, a retirada, a recusa e a devolução são fatos do stream. Uma devolução acrescenta um novo fato em vez de apagar ou alterar a retirada anterior.

O ADR-006 reúne as decisões de resiliência e a Saga do pagamento recusado. A Saga é coreografada, e a liberação referencia explicitamente a reserva que precisa ser desfeita. Falhas transitórias podem ser tentadas novamente e, quando o processamento não consegue prosseguir, existe um caminho separado de falha e reprocessamento.

O ADR-007 separa a fonte da verdade das estruturas destinadas à leitura. As projeções podem ser reconstruídas a partir do histórico e não participam da decisão de aceitar ou recusar uma retirada de estoque.

## 5. Quando falha

<!-- Pessoa A -->

## 6. Quando cresce

<!-- Pessoa A -->

## 7. O que se enxerga

<!-- Pessoa A -->

## 8. O que ficou de fora

<!-- Pessoa A -->