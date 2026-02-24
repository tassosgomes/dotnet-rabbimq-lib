---
status: pending
parallelizable: false
blocked_by: [5.0]
---

<task_context>
<domain>engine/messaging</domain>
<type>implementation</type>
<scope>middleware</scope>
<complexity>medium</complexity>
<dependencies>task_5</dependencies>
<unblocks>"7.0"</unblocks>
</task_context>

# Tarefa 6.0: DI Extensions — AddRmqTopicConsumer

## Visao Geral

Adicionar o metodo de extensao `AddRmqTopicConsumer<TMessage, THandler>` em `ServiceCollectionExtensions` para registrar consumers inscritos em Topic Exchanges via dependency injection. O metodo existente `AddRmqConsumer` permanece inalterado.

<requirements>
- Adicionar novo metodo em `Extensions/ServiceCollectionExtensions.cs`:
  - `AddRmqTopicConsumer<TMessage, THandler>(services, Action<TopicSubscriptionOptions> configure)`
  - Recebe delegate de configuracao para `TopicSubscriptionOptions`
  - Valida: `ExchangeName` nao null/empty, `BindingPatterns` nao null/empty, `QueueName` nao null/empty
  - Registra `IRmqMessageHandler<TMessage>` como Transient
  - Registra `RmqTopicConsumer<TMessage>` como `IHostedService` via factory delegate
  - Resolve dependencias do container: IRmqConnectionManager, IQueueManager, ICloudEventWrapper, RmqOptions, ILogger
- Metodo existente `AddRmqConsumer` NAO deve ser alterado
- Metodo existente `AddRmqCloudEvents` NAO deve ser alterado
- Testes unitarios de registro e validacao
</requirements>

## Subtarefas

- [ ] 6.1 Implementar `AddRmqTopicConsumer<TMessage, THandler>` em `ServiceCollectionExtensions.cs`
- [ ] 6.2 Validacao de parametros: ExchangeName, BindingPatterns, QueueName
- [ ] 6.3 Testes unitarios: `IRmqMessageHandler<TMessage>` registrado corretamente
- [ ] 6.4 Testes unitarios: `IHostedService` registrado (RmqTopicConsumer)
- [ ] 6.5 Testes unitarios: excecao se `ExchangeName` vazio
- [ ] 6.6 Testes unitarios: excecao se `BindingPatterns` vazio
- [ ] 6.7 Testes unitarios: excecao se `QueueName` vazio
- [ ] 6.8 Testes unitarios: multiplos `AddRmqTopicConsumer` com handlers distintos
- [ ] 6.9 Validar que testes existentes de `AddRmqCloudEvents` e `AddRmqConsumer` continuam passando

## Sequenciamento

- Bloqueado por: 5.0
- Desbloqueia: 7.0
- Paralelizavel: Nao (depende do consumer)

## Detalhes de Implementacao

Ref: techspec secao 6.7 (ServiceCollectionExtensions).

**Implementacao:**
```csharp
public static IServiceCollection AddRmqTopicConsumer<TMessage, THandler>(
    this IServiceCollection services,
    Action<TopicSubscriptionOptions> configure)
    where TMessage : class
    where THandler : class, IRmqMessageHandler<TMessage>
{
    ArgumentNullException.ThrowIfNull(services);
    ArgumentNullException.ThrowIfNull(configure);

    var subscription = new TopicSubscriptionOptions
    {
        ExchangeName = null!,
        BindingPatterns = null!
    };
    configure(subscription);

    // Validacoes
    ArgumentException.ThrowIfNullOrWhiteSpace(subscription.ExchangeName);
    if (subscription.BindingPatterns is null || subscription.BindingPatterns.Count == 0)
        throw new ArgumentException("At least one binding pattern is required.");
    if (string.IsNullOrWhiteSpace(subscription.QueueName))
        throw new ArgumentException("QueueName is required for durable topic consumers.");

    services.AddTransient<IRmqMessageHandler<TMessage>, THandler>();
    services.AddHostedService(sp =>
        new RmqTopicConsumer<TMessage>(
            sp.GetRequiredService<IRmqConnectionManager>(),
            sp.GetRequiredService<IQueueManager>(),
            sp.GetRequiredService<ICloudEventWrapper>(),
            sp.GetRequiredService<IRmqMessageHandler<TMessage>>(),
            sp.GetRequiredService<RmqOptions>(),
            subscription,
            sp.GetService<ILogger<RmqTopicConsumer<TMessage>>>()));

    return services;
}
```

**Cenario de uso:**
```csharp
builder.Services.AddRmqCloudEvents(options => { /* connection, cloudevents, etc */ });

builder.Services.AddRmqTopicConsumer<Order, OrderAuditHandler>(opts =>
{
    opts.ExchangeName = "business-events";
    opts.QueueName = "order-audit-queue";
    opts.BindingPatterns = ["orders.*"];
});

builder.Services.AddRmqTopicConsumer<Payment, PaymentHandler>(opts =>
{
    opts.ExchangeName = "business-events";
    opts.QueueName = "payment-processing-queue";
    opts.BindingPatterns = ["payments.*"];
});
```

## Criterios de Sucesso

- `AddRmqTopicConsumer` registra handler e hosted service corretamente
- Validacao lanca `ArgumentException` para parametros invalidos
- Multiplos consumers de tipos diferentes podem ser registrados
- Metodos existentes `AddRmqCloudEvents` e `AddRmqConsumer` nao alterados
- Testes existentes de DI continuam passando
