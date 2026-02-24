# Resumo de Tarefas de Implementacao de Topic Exchange (Pub/Sub)

## Visao Geral

Adicionar suporte a **Topic Exchange** na biblioteca `Rmq.CloudEvents`, permitindo arquitetura pub/sub onde produtores publicam mensagens com routing keys hierarquicas e consumidores se inscrevem em topicos de interesse via binding patterns com wildcards. Todas as alteracoes sao **aditivas** — zero breaking changes na API existente.

## Fases de Implementacao

### Fase 1 - Fundacao (Modelos e Infraestrutura)
Criar os modelos de configuracao (`ExchangeOptions`, `TopicSubscriptionOptions`) e estender a infraestrutura (`QueueManager`) para declarar Topic Exchanges com bindings. Essa fase estabelece a base sobre a qual Publisher e Consumer serao construidos.

### Fase 2 - Publisher e Consumer
Implementar os novos metodos `PublishToTopicAsync` no Publisher, estender o `MessageContext` e `RmqAsyncConsumerHandler` para repassar exchange/routingKey, e criar o `RmqTopicConsumer<T>` como hosted service.

### Fase 3 - Integracao e Validacao
Registrar os novos componentes via DI (`AddRmqTopicConsumer`), criar testes de integracao com Testcontainers e atualizar o sample application com cenario pub/sub.

## Tarefas

- [x] 1.0 Modelos de Configuracao (ExchangeOptions, TopicSubscriptionOptions, RmqOptions.Exchanges)
- [x] 2.0 QueueManager — DeclareExchangeAndBindingsAsync
- [ ] 3.0 Publisher — PublishToTopicAsync
- [ ] 4.0 MessageContext e RmqAsyncConsumerHandler — Exchange/RoutingKey
- [ ] 5.0 RmqTopicConsumer — Hosted Service para Topic Exchange
- [ ] 6.0 DI Extensions — AddRmqTopicConsumer
- [ ] 7.0 Testes de Integracao e Sample Application

## Analise de Paralelizacao

### Lanes de Execucao Paralela

| Lane | Tarefas | Descricao |
|------|---------|-----------|
| Lane A | 1.0 → 2.0 → 3.0 | Config → Infraestrutura → Publisher |
| Lane B | 4.0 (apos 1.0) | MessageContext + Handler (paralelo a 2.0/3.0) |
| Merge | 5.0 (apos 2.0 + 4.0) → 6.0 → 7.0 | Consumer → DI → Integracao |

### Caminho Critico

```
1.0 → 2.0 → 3.0 ──┐
                    ├── 5.0 → 6.0 → 7.0
1.0 → 4.0 ─────────┘
```

Tempo minimo: 6 tarefas em sequencia (1 → 2 → 3/4 → 5 → 6 → 7), com 3.0 e 4.0 paralelizaveis.

### Diagrama de Dependencias

```
  ┌───────┐
  │ 1.0   │  Modelos de Configuracao
  │ Config│
  └──┬──┬─┘
     │  │
     │  └──────────────┐
     ▼                 ▼
  ┌───────┐        ┌───────┐
  │ 2.0   │        │ 4.0   │
  │ Queue  │        │ Context│  ← Paralelizaveis
  │Manager │        │+Handler│
  └──┬─────┘        └──┬────┘
     │                  │
     │   ┌──────┐       │
     ├──►│ 3.0  │       │
     │   │Publi-│       │
     │   │sher  │       │
     │   └──┬───┘       │
     │      │           │
     ▼      ▼           ▼
  ┌─────────────────────────┐
  │         5.0             │
  │    RmqTopicConsumer     │
  └────────────┬────────────┘
               │
               ▼
  ┌─────────────────────────┐
  │         6.0             │
  │    DI Extensions        │
  └────────────┬────────────┘
               │
               ▼
  ┌─────────────────────────┐
  │         7.0             │
  │  Integracao + Sample    │
  └─────────────────────────┘
```
