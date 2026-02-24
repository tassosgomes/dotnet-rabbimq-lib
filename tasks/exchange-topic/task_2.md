---
status: pending
parallelizable: false
blocked_by: [1.0]
---

<task_context>
<domain>engine/messaging</domain>
<type>implementation</type>
<scope>core_feature</scope>
<complexity>medium</complexity>
<dependencies>task_1</dependencies>
<unblocks>"3.0, 5.0"</unblocks>
</task_context>

# Tarefa 2.0: QueueManager — DeclareExchangeAndBindingsAsync

## Visao Geral

Estender `IQueueManager` e `QueueManager` com um novo metodo `DeclareExchangeAndBindingsAsync` que declara uma Topic Exchange, a queue do consumer (com DLQ via metodo existente) e cria os bindings para cada pattern. O metodo existente `DeclareQueueWithDlqAsync` permanece inalterado e eh reutilizado internamente.

<requirements>
- Adicionar novo metodo na interface `Infrastructure/IQueueManager.cs`:
  - `DeclareExchangeAndBindingsAsync(channel, exchangeName, queueName, bindingPatterns, queueOptions, exchangeOptions?, cancellationToken)`
- Implementar em `Infrastructure/QueueManager.cs`:
  - Declarar exchange com `ExchangeType.Topic`, durable, nao auto-delete (ou via ExchangeOptions)
  - Reutilizar `DeclareQueueWithDlqAsync` para declarar a queue + DLQ
  - Criar bind da queue na exchange para cada pattern em `bindingPatterns`
  - Validar que `bindingPatterns` contem pelo menos um item
- Testes unitarios com mock de `IChannel`
- Metodo existente `DeclareQueueWithDlqAsync` NAO deve ser alterado
</requirements>

## Subtarefas

- [ ] 2.1 Adicionar assinatura `DeclareExchangeAndBindingsAsync` em `IQueueManager.cs`
- [ ] 2.2 Implementar `DeclareExchangeAndBindingsAsync` em `QueueManager.cs`
- [ ] 2.3 Testes unitarios: exchange declarada como `topic` com parametros corretos
- [ ] 2.4 Testes unitarios: `DeclareQueueWithDlqAsync` chamado para a queue do consumer
- [ ] 2.5 Testes unitarios: bindings criados para cada pattern
- [ ] 2.6 Testes unitarios: validacao de patterns vazio lanca excecao
- [ ] 2.7 Testes unitarios: `exchangeOptions` null usa defaults (durable=true, autoDelete=false)
- [ ] 2.8 Validar que testes existentes do QueueManager continuam passando

## Sequenciamento

- Bloqueado por: 1.0
- Desbloqueia: 3.0, 5.0
- Paralelizavel: Sim (com 4.0, apos 1.0 completado)

## Detalhes de Implementacao

Ref: techspec secoes 6.2 (IQueueManager), 6.3 (QueueManager).

**Fluxo de declaracao:**
```
ExchangeDeclareAsync("business-events", type: topic)
    │
    ├── DeclareQueueWithDlqAsync("order-audit-queue", ...)
    │       ├── ExchangeDeclare("order-audit-queue.dlx", direct)
    │       ├── QueueDeclare("order-audit-queue.dlq", quorum)
    │       ├── QueueBind(dlq -> dlx)
    │       └── QueueDeclare("order-audit-queue", quorum, x-dead-letter-exchange=dlx)
    │
    ├── QueueBind("order-audit-queue" -> "business-events", routingKey: "orders.*")
    └── QueueBind("order-audit-queue" -> "business-events", routingKey: "payments.#")
```

**Assinatura do metodo:**
```csharp
Task DeclareExchangeAndBindingsAsync(
    IChannel channel,
    string exchangeName,
    string queueName,
    IReadOnlyList<string> bindingPatterns,
    QueueOptions queueOptions,
    ExchangeOptions? exchangeOptions = null,
    CancellationToken cancellationToken = default);
```

**Validacoes:**
- `channel` nao null
- `exchangeName` nao null/empty
- `queueName` nao null/empty
- `bindingPatterns` nao null e count > 0

## Criterios de Sucesso

- Exchange declarada como `ExchangeType.Topic`
- Queue principal e DLQ declaradas via `DeclareQueueWithDlqAsync` (reutilizacao)
- Um `QueueBindAsync` executado para cada binding pattern
- Excecao lancada se `bindingPatterns` estiver vazio
- ExchangeOptions null resulta em defaults (durable=true, autoDelete=false)
- Metodo existente `DeclareQueueWithDlqAsync` nao foi alterado
- Testes existentes do QueueManager continuam passando
- Testes novos cobrem todos os cenarios
