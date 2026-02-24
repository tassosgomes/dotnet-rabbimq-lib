---
status: pending
parallelizable: true
blocked_by: [1.0]
---

<task_context>
<domain>engine/messaging</domain>
<type>implementation</type>
<scope>core_feature</scope>
<complexity>low</complexity>
<dependencies>task_1</dependencies>
<unblocks>"5.0"</unblocks>
</task_context>

# Tarefa 4.0: MessageContext e RmqAsyncConsumerHandler — Exchange/RoutingKey

## Visao Geral

Estender `MessageContext` com novas propriedades `ExchangeName` e `RoutingKey` e alterar `RmqAsyncConsumerHandler` para repassar esses valores ao contexto. Ambas as propriedades tem defaults (`string.Empty`) garantindo backward-compatibility — handlers existentes nao precisam ser alterados.

<requirements>
- Alterar `Consuming/MessageContext.cs`:
  - Nova propriedade `ExchangeName` (string, init, default `string.Empty`)
  - Nova propriedade `RoutingKey` (string, init, default `string.Empty`)
  - Propriedades existentes NAO devem ser alteradas
- Alterar `Consuming/RmqAsyncConsumerHandler.cs`:
  - Metodo `CreateMessageContext`: adicionar parametros `exchange` e `routingKey`
  - Repassar `exchange` e `routingKey` do `HandleBasicDeliverAsync` para o `CreateMessageContext`
  - Preencher `ExchangeName` e `RoutingKey` no `MessageContext`
- Testes unitarios que validam preenchimento dos novos campos
- Testes existentes devem continuar passando (defaults vazios)
</requirements>

## Subtarefas

- [ ] 4.1 Adicionar propriedades `ExchangeName` e `RoutingKey` em `MessageContext.cs`
- [ ] 4.2 Alterar `CreateMessageContext` em `RmqAsyncConsumerHandler.cs` para receber e repassar exchange/routingKey
- [ ] 4.3 Alterar chamada de `CreateMessageContext` em `HandleBasicDeliverAsync` para passar os parametros
- [ ] 4.4 Testes unitarios: `MessageContext` com defaults vazios (backward-compat)
- [ ] 4.5 Testes unitarios: `RmqAsyncConsumerHandler` preenche `ExchangeName` e `RoutingKey` no context
- [ ] 4.6 Testes unitarios: handler recebe exchange/routingKey corretos via MessageContext
- [ ] 4.7 Validar que testes existentes do consumer/handler continuam passando

## Sequenciamento

- Bloqueado por: 1.0
- Desbloqueia: 5.0
- Paralelizavel: Sim (com 2.0 e 3.0, apos 1.0)

## Detalhes de Implementacao

Ref: techspec secoes 4.4 (MessageContext) e 6.6 (RmqAsyncConsumerHandler).

**MessageContext (alteracao):**
```csharp
public sealed class MessageContext
{
    // ... propriedades existentes (inalteradas) ...

    /// <summary>
    /// Nome da exchange de onde a mensagem foi recebida.
    /// Vazio para mensagens da default exchange (publish direto em queue).
    /// </summary>
    public string ExchangeName { get; init; } = string.Empty;

    /// <summary>
    /// Routing key original da mensagem.
    /// Para publish direto em queue, corresponde ao nome da queue.
    /// </summary>
    public string RoutingKey { get; init; } = string.Empty;
}
```

**RmqAsyncConsumerHandler — alteracao em HandleBasicDeliverAsync:**

O metodo `HandleBasicDeliverAsync` ja recebe `string exchange` e `string routingKey` como parametros do RabbitMQ.Client. A alteracao eh apenas repassar para `CreateMessageContext`:

```csharp
// Antes:
var context = CreateMessageContext(metadata, headers, deliveryTag, currentAttempt, redelivered);

// Depois:
var context = CreateMessageContext(metadata, headers, deliveryTag, currentAttempt, redelivered, exchange, routingKey);
```

**CreateMessageContext — nova assinatura:**
```csharp
private MessageContext CreateMessageContext(
    CloudEventMetadata metadata,
    IReadOnlyDictionary<string, object> headers,
    ulong deliveryTag,
    int currentAttempt,
    bool redelivered,
    string exchange,       // novo parametro
    string routingKey)     // novo parametro
{
    return new MessageContext
    {
        // ... campos existentes ...
        ExchangeName = exchange,      // novo
        RoutingKey = routingKey        // novo
    };
}
```

## Criterios de Sucesso

- `MessageContext.ExchangeName` default = `string.Empty`
- `MessageContext.RoutingKey` default = `string.Empty`
- Handlers existentes continuam compilando e funcionando sem alteracao
- `RmqAsyncConsumerHandler` repassa `exchange` e `routingKey` corretamente
- Para consume direto de queue (sem topic), `ExchangeName` = "" e `RoutingKey` = nome da queue
- Testes existentes do consumer/handler continuam passando
