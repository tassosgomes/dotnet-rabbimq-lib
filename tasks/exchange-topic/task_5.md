---
status: pending
parallelizable: false
blocked_by: [2.0, 4.0]
---

<task_context>
<domain>engine/messaging</domain>
<type>implementation</type>
<scope>core_feature</scope>
<complexity>high</complexity>
<dependencies>task_2, task_4</dependencies>
<unblocks>"6.0"</unblocks>
</task_context>

# Tarefa 5.0: RmqTopicConsumer — Hosted Service para Topic Exchange

## Visao Geral

Criar `RmqTopicConsumer<T>`, um hosted service (`IHostedService`) que consome mensagens de uma queue inscrita em uma Topic Exchange. Utiliza `TopicSubscriptionOptions` para configuracao, `QueueManager.DeclareExchangeAndBindingsAsync` para declarar a topologia e `RmqAsyncConsumerHandler<T>` para processar mensagens (reutilizando toda a logica de retry, ACK/NACK e CloudEvents existente).

<requirements>
- Criar `Consuming/RmqTopicConsumer.cs`:
  - Classe `internal sealed`, generica `<T>` com restricao `where T : class`
  - Implementa `IHostedService` e `IRmqConsumer`
  - Recebe via construtor: IRmqConnectionManager, IQueueManager, ICloudEventWrapper, IRmqMessageHandler<T>, RmqOptions, TopicSubscriptionOptions, ILogger?
  - `StartAsync`: cria channel, chama `DeclareExchangeAndBindingsAsync`, cria `RmqAsyncConsumerHandler<T>`, inicia `BasicConsumeAsync`
  - `StopAsync`: cancela consumer, fecha channel (mesma logica do `RmqConsumer<T>` existente)
  - `DisposeAsync`: chama StopAsync, dispoe lifecycle lock
  - Thread-safe via `SemaphoreSlim` (lifecycle lock)
  - Idempotente: `StartAsync` repetido nao recria canal se ja aberto
- Logging:
  - Information: consumer iniciado (exchange, queue, patterns, tag)
  - Information: consumer parado (exchange, queue)
- Testes unitarios com mocks
</requirements>

## Subtarefas

- [ ] 5.1 Criar classe `RmqTopicConsumer<T>` com construtor e validacoes
- [ ] 5.2 Implementar `StartAsync` — channel, declare exchange+bindings, consume
- [ ] 5.3 Implementar `StopAsync` — cancel, close, dispose channel
- [ ] 5.4 Implementar `DisposeAsync`
- [ ] 5.5 Testes unitarios: `StartAsync` chama `DeclareExchangeAndBindingsAsync` com parametros corretos
- [ ] 5.6 Testes unitarios: `StartAsync` chama `BasicConsumeAsync` na queue correta
- [ ] 5.7 Testes unitarios: `StartAsync` idempotente (nao recria canal se ja aberto)
- [ ] 5.8 Testes unitarios: `StopAsync` cancela consumer e fecha channel
- [ ] 5.9 Testes unitarios: `ExchangeOptions` do `RmqOptions` sao usadas quando disponiveis
- [ ] 5.10 Testes unitarios: construtor valida argumentos (nulls)

## Sequenciamento

- Bloqueado por: 2.0 (DeclareExchangeAndBindingsAsync), 4.0 (MessageContext com exchange/routingKey)
- Desbloqueia: 6.0
- Paralelizavel: Nao neste ponto (depende de 2.0 e 4.0)

## Detalhes de Implementacao

Ref: techspec secao 6.5 (RmqTopicConsumer) e fluxo 7.2 (Topic Consumer Startup).

**Estrutura da classe:**
```csharp
internal sealed class RmqTopicConsumer<T> : IHostedService, IRmqConsumer
    where T : class
{
    private readonly IRmqConnectionManager _connectionManager;
    private readonly IQueueManager _queueManager;
    private readonly ICloudEventWrapper _cloudEventWrapper;
    private readonly IRmqMessageHandler<T> _messageHandler;
    private readonly RmqOptions _options;
    private readonly TopicSubscriptionOptions _subscription;
    private readonly ILogger<RmqTopicConsumer<T>> _logger;
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);
    private IChannel? _channel;
    private string? _consumerTag;
    private bool _isStarted;
}
```

**StartAsync flow:**
1. Adquirir lifecycle lock
2. Se ja iniciado e canal aberto, retornar (idempotente)
3. `GetConnectionAsync` + `CreateChannelAsync`
4. Resolver `ExchangeOptions` de `RmqOptions.Exchanges` (ou null)
5. `DeclareExchangeAndBindingsAsync(channel, exchangeName, queueName, patterns, queueOpts, exchangeOpts)`
6. Criar `RmqAsyncConsumerHandler<T>(channel, handler, cloudEventWrapper, retryOptions, queueName, logger)`
7. `BasicConsumeAsync(queue, autoAck:false, consumer:handler)`
8. Guardar channel, consumerTag, isStarted=true
9. Log Information

**Semelhanca com RmqConsumer<T>:**
A classe segue a mesma estrutura do `RmqConsumer<T>` existente. A diferenca principal eh:
- `RmqConsumer<T>` chama `DeclareQueueWithDlqAsync` (queue direta)
- `RmqTopicConsumer<T>` chama `DeclareExchangeAndBindingsAsync` (exchange + queue + bindings)

O `RmqAsyncConsumerHandler<T>` eh reutilizado sem alteracao (ja recebe exchange/routingKey).

## Criterios de Sucesso

- Consumer inicia e se inscreve na queue correta
- Exchange, queue, DLQ e bindings sao declarados antes do consume
- Multiple binding patterns resultam em multiplos binds
- Consumer eh idempotente no start
- Stop cancela consumer e fecha channel corretamente
- DisposeAsync funciona mesmo com stop repetido
- Testes unitarios cobrem todos os cenarios
