# Technical Specification: Suporte a Topic Exchange (Pub/Sub)

## 1. Informacoes do Documento

| Campo | Valor |
|---|---|
| **Titulo** | Tech Spec - Suporte a Topic Exchange para Arquitetura Pub/Sub |
| **Versao** | 1.0 |
| **Data** | 24 de Fevereiro de 2026 |
| **Tech Spec Base** | [tasks/rabbitmq-client/techspec.md](../rabbitmq-client/techspec.md) |
| **Status** | Draft |

---

## 2. Visao Geral

A biblioteca `Rmq.CloudEvents` atualmente opera exclusivamente via **default exchange** (`""`) do RabbitMQ, publicando diretamente em queues pelo nome. Esse modelo atende cenarios ponto-a-ponto, mas nao suporta **pub/sub** — onde produtores publicam em topicos e multiplos consumidores se inscrevem nos topicos de interesse.

Esta especificacao adiciona suporte a **Topic Exchange**, o padrao mais flexivel do RabbitMQ para pub/sub, onde:

- **Produtores** publicam mensagens em uma exchange do tipo `topic` com uma **routing key** hierarquica (ex: `orders.created`, `orders.updated`, `payments.completed`).
- **Consumidores** declaram queues e fazem **bind** na exchange com **binding patterns** usando wildcards (`*` = uma palavra, `#` = zero ou mais palavras).

**Requisito critico:** a implementacao deve ser **backward-compatible** — toda a API existente (publish direto em queue, consume direto de queue) deve continuar funcionando sem alteracao.

---

## 3. Motivacao e Cenarios

### 3.1 Cenarios Atendidos

| Cenario | Routing Key (Publish) | Binding Pattern (Consumer) | Resultado |
|---|---|---|---|
| Servico de pedidos emite eventos | `orders.created` | `orders.*` | Recebe todos os eventos de orders |
| Servico de auditoria escuta tudo | `orders.created` | `#` | Recebe todas as mensagens da exchange |
| Servico de pagamento escuta criacao | `orders.created` | `orders.created` | Recebe apenas `orders.created` |
| Multiplos topicos por consumer | `orders.updated` | `orders.*` | Um consumer recebe created + updated |
| Fanout seletivo | `payments.completed.br` | `payments.*.br` | Recebe pagamentos do Brasil |

### 3.2 O Que Nao Muda

- `PublishAsync(queueName, payload)` continua publicando na default exchange (direto na queue).
- `AddRmqConsumer<T, THandler>(queueName)` continua consumindo de uma queue nomeada sem exchange.
- Toda a logica de DLQ, retry, CloudEvents, serializacao permanece identica.

---

## 4. Design da API Publica (Backward-Compatible)

### 4.1 Principio: Aditividade

Todas as mudancas sao **aditivas** — novas classes, novas interfaces, novos metodos, novos overloads. Nenhuma assinatura existente eh alterada ou removida.

### 4.2 Novos Modelos de Configuracao

#### 4.2.1 ExchangeOptions

```csharp
namespace Rmq.CloudEvents.Configuration;

/// <summary>
/// Configuracoes de uma exchange do tipo Topic.
/// </summary>
public sealed class ExchangeOptions
{
    /// <summary>
    /// Nome da exchange.
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    /// Indica se a exchange eh duravel (sobrevive a restart do broker).
    /// Default: true.
    /// </summary>
    public bool Durable { get; set; } = true;

    /// <summary>
    /// Indica se a exchange eh deletada automaticamente quando nao ha mais bindings.
    /// Default: false.
    /// </summary>
    public bool AutoDelete { get; set; } = false;

    /// <summary>
    /// Argumentos adicionais para a exchange.
    /// </summary>
    public IDictionary<string, object>? Arguments { get; set; }
}
```

> **Nota:** O tipo da exchange sera sempre `topic` — a classe nao expoe `Type` porque o escopo desta feature eh exclusivamente Topic Exchange. Isso simplifica a API e evita configuracao incorreta.

#### 4.2.2 TopicSubscriptionOptions

```csharp
namespace Rmq.CloudEvents.Configuration;

/// <summary>
/// Configuracoes para inscricao de um consumer em uma Topic Exchange.
/// </summary>
public sealed class TopicSubscriptionOptions
{
    /// <summary>
    /// Nome da exchange topic a qual se inscrever.
    /// </summary>
    public required string ExchangeName { get; set; }

    /// <summary>
    /// Nome da queue que sera criada/usada para este consumer.
    /// Se nao informado, o RabbitMQ gera um nome exclusivo (queue anonima).
    /// Recomendado: usar nomes fixos para durabilidade.
    /// </summary>
    public string? QueueName { get; set; }

    /// <summary>
    /// Routing key patterns para binding na exchange.
    /// Deve conter pelo menos um pattern.
    /// Exemplos: "orders.*", "orders.created", "payments.#", "#"
    /// </summary>
    public required IReadOnlyList<string> BindingPatterns { get; set; }

    /// <summary>
    /// Configuracoes da queue (quorum size, delivery limit, retry, DLQ).
    /// Usa defaults se nao informado.
    /// </summary>
    public QueueOptions Queue { get; set; } = new();
}
```

#### 4.2.3 Extensao de RmqOptions

```csharp
// Propriedade adicionada a RmqOptions (existente)
public sealed class RmqOptions
{
    // ... propriedades existentes (nao alteradas) ...

    /// <summary>
    /// Configuracoes de exchanges topic registradas.
    /// Key: nome logico da exchange. Value: opcoes da exchange.
    /// </summary>
    public Dictionary<string, ExchangeOptions> Exchanges { get; set; } = new();
}
```

### 4.3 Extensao de IRmqPublisher

Novos metodos sao adicionados a interface existente com **default interface methods** para nao quebrar implementacoes custom (caso existam):

```csharp
namespace Rmq.CloudEvents.Publishing;

public interface IRmqPublisher : IAsyncDisposable
{
    // --- Metodos existentes (inalterados) ---

    Task PublishAsync<T>(
        string queueName,
        T payload,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class;

    Task PublishAsync<T>(
        string queueName,
        T payload,
        IDictionary<string, object> headers,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class;

    // --- Novos metodos para Topic Exchange ---

    /// <summary>
    /// Publica um payload em uma Topic Exchange com a routing key especificada.
    /// </summary>
    /// <typeparam name="T">Tipo do payload.</typeparam>
    /// <param name="exchangeName">Nome da exchange topic destino.</param>
    /// <param name="routingKey">Routing key hierarquica (ex: "orders.created").</param>
    /// <param name="payload">Payload a publicar.</param>
    /// <param name="cloudEventType">Tipo opcional do CloudEvent.</param>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    Task PublishToTopicAsync<T>(
        string exchangeName,
        string routingKey,
        T payload,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    /// Publica um payload em uma Topic Exchange com routing key e headers customizados.
    /// </summary>
    Task PublishToTopicAsync<T>(
        string exchangeName,
        string routingKey,
        T payload,
        IDictionary<string, object> headers,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class;
}
```

### 4.4 Extensao de MessageContext

Novas propriedades opcionais sao adicionadas ao `MessageContext` existente:

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

### 4.5 Novo Metodo de Registro de Consumer (DI)

```csharp
public static class ServiceCollectionExtensions
{
    // --- Metodo existente (inalterado) ---
    // AddRmqConsumer<TMessage, THandler>(queueName)

    // --- Novo metodo para Topic Exchange ---

    /// <summary>
    /// Registra um consumer inscrito em uma Topic Exchange.
    /// </summary>
    /// <typeparam name="TMessage">Tipo da mensagem.</typeparam>
    /// <typeparam name="THandler">Tipo do handler.</typeparam>
    /// <param name="services">Colecao de servicos.</param>
    /// <param name="configure">Delegate de configuracao da inscricao.</param>
    /// <returns>A propria colecao para encadeamento.</returns>
    public static IServiceCollection AddRmqTopicConsumer<TMessage, THandler>(
        this IServiceCollection services,
        Action<TopicSubscriptionOptions> configure)
        where TMessage : class
        where THandler : class, IRmqMessageHandler<TMessage>;
}
```

---

## 5. Cenarios de Uso da Nova API

### 5.1 Configuracao e Publish em Topic Exchange

```csharp
// Program.cs
builder.Services.AddRmqCloudEvents(options =>
{
    options.Connection = new RmqConnectionOptions
    {
        HostName = "localhost",
        UserName = "guest",
        Password = "guest"
    };

    options.DefaultCloudEvents = new CloudEventsOptions
    {
        Source = new Uri("/order-service", UriKind.Relative),
        DefaultType = "com.mycompany.events"
    };

    // Registrar exchanges topic
    options.Exchanges.Add("business-events", new ExchangeOptions
    {
        Name = "business-events",
        Durable = true
    });
});
```

```csharp
// OrderService.cs — Produtor
public class OrderService
{
    private readonly IRmqPublisher _publisher;

    public OrderService(IRmqPublisher publisher)
    {
        _publisher = publisher;
    }

    public async Task PlaceOrderAsync(Order order, CancellationToken ct)
    {
        // Publica na exchange "business-events" com routing key "orders.created"
        await _publisher.PublishToTopicAsync(
            exchangeName: "business-events",
            routingKey: "orders.created",
            payload: order,
            cancellationToken: ct);
    }

    public async Task UpdateOrderAsync(Order order, CancellationToken ct)
    {
        await _publisher.PublishToTopicAsync(
            exchangeName: "business-events",
            routingKey: "orders.updated",
            payload: order,
            cancellationToken: ct);
    }
}
```

### 5.2 Consumer Inscrito em Topicos

```csharp
// Registro — Consumer que recebe TODOS os eventos de orders
builder.Services.AddRmqTopicConsumer<Order, OrderAuditHandler>(options =>
{
    options.ExchangeName = "business-events";
    options.QueueName = "order-audit-queue";    // queue nomeada (duravel)
    options.BindingPatterns = ["orders.*"];       // recebe orders.created, orders.updated, etc.
});

// Registro — Consumer que recebe APENAS orders.created
builder.Services.AddRmqTopicConsumer<Order, NewOrderHandler>(options =>
{
    options.ExchangeName = "business-events";
    options.QueueName = "new-order-processing-queue";
    options.BindingPatterns = ["orders.created"];
});

// Registro — Consumer que recebe TUDO da exchange
builder.Services.AddRmqTopicConsumer<BaseEvent, GlobalAuditHandler>(options =>
{
    options.ExchangeName = "business-events";
    options.QueueName = "global-audit-queue";
    options.BindingPatterns = ["#"];
    options.Queue = new QueueOptions
    {
        DeliveryLimit = 10,
        Retry = new RetryOptions { MaxAttempts = 3 }
    };
});
```

### 5.3 Handler — Acesso ao RoutingKey e Exchange

```csharp
public class OrderAuditHandler : IRmqMessageHandler<Order>
{
    private readonly ILogger<OrderAuditHandler> _logger;

    public OrderAuditHandler(ILogger<OrderAuditHandler> logger)
    {
        _logger = logger;
    }

    public Task HandleAsync(Order message, MessageContext context, CancellationToken ct)
    {
        // RoutingKey permite saber qual topico originou a mensagem
        _logger.LogInformation(
            "Evento {RoutingKey} recebido na exchange {Exchange}. OrderId={OrderId}",
            context.RoutingKey,      // ex: "orders.created"
            context.ExchangeName,    // ex: "business-events"
            message.OrderId);

        return Task.CompletedTask;
    }
}
```

### 5.4 API Existente Continua Funcionando (Sem Breaking Changes)

```csharp
// Tudo abaixo continua funcionando exatamente como antes:

// Publish direto em queue (default exchange)
await publisher.PublishAsync("orders", order);

// Consumer direto de queue
builder.Services.AddRmqConsumer<Order, OrderHandler>("orders");
```

---

## 6. Componentes Internos e Detalhamento

### 6.1 Visao Geral dos Componentes Alterados/Novos

| Componente | Tipo | Descricao |
|---|---|---|
| `ExchangeOptions` | **Novo** | Modelo de configuracao para exchanges |
| `TopicSubscriptionOptions` | **Novo** | Modelo de configuracao para subscricoes topic |
| `RmqOptions.Exchanges` | **Alterado** | Nova propriedade adicionada |
| `IRmqPublisher` | **Alterado** | Novos metodos `PublishToTopicAsync` |
| `RmqPublisher` | **Alterado** | Implementacao dos novos metodos |
| `IQueueManager` | **Alterado** | Novo metodo `DeclareExchangeAndBindingsAsync` |
| `QueueManager` | **Alterado** | Implementacao da declaracao de exchange + bindings |
| `MessageContext` | **Alterado** | Novas propriedades `ExchangeName`, `RoutingKey` |
| `RmqAsyncConsumerHandler` | **Alterado** | Repassa `exchange` e `routingKey` para o context |
| `ServiceCollectionExtensions` | **Alterado** | Novo metodo `AddRmqTopicConsumer` |
| `RmqTopicConsumer<T>` | **Novo** | Hosted service para consume via topic exchange |

### 6.2 IQueueManager — Novo Metodo

```csharp
internal interface IQueueManager
{
    // Metodo existente (inalterado)
    Task DeclareQueueWithDlqAsync(
        IChannel channel,
        string queueName,
        QueueOptions options,
        CancellationToken cancellationToken = default);

    // Novo metodo
    /// <summary>
    /// Declara uma Topic Exchange, a queue do consumer, DLQ e todos os bindings.
    /// </summary>
    /// <param name="channel">Canal RabbitMQ.</param>
    /// <param name="exchangeName">Nome da exchange topic.</param>
    /// <param name="queueName">Nome da queue do consumer.</param>
    /// <param name="bindingPatterns">Routing key patterns para binding.</param>
    /// <param name="queueOptions">Configuracoes da queue.</param>
    /// <param name="exchangeOptions">Configuracoes da exchange (null usa defaults).</param>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    Task DeclareExchangeAndBindingsAsync(
        IChannel channel,
        string exchangeName,
        string queueName,
        IReadOnlyList<string> bindingPatterns,
        QueueOptions queueOptions,
        ExchangeOptions? exchangeOptions = null,
        CancellationToken cancellationToken = default);
}
```

### 6.3 QueueManager — Implementacao

```csharp
internal sealed class QueueManager : IQueueManager
{
    // Metodo existente (inalterado)
    public async Task DeclareQueueWithDlqAsync(...) { /* ... */ }

    // Novo metodo
    public async Task DeclareExchangeAndBindingsAsync(
        IChannel channel,
        string exchangeName,
        string queueName,
        IReadOnlyList<string> bindingPatterns,
        QueueOptions queueOptions,
        ExchangeOptions? exchangeOptions = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentException.ThrowIfNullOrWhiteSpace(exchangeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        ArgumentNullException.ThrowIfNull(bindingPatterns);

        if (bindingPatterns.Count == 0)
        {
            throw new ArgumentException("At least one binding pattern is required.", nameof(bindingPatterns));
        }

        // 1. Declarar a Topic Exchange
        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Topic,
            durable: exchangeOptions?.Durable ?? true,
            autoDelete: exchangeOptions?.AutoDelete ?? false,
            arguments: exchangeOptions?.Arguments?.ToDictionary(
                kvp => kvp.Key, kvp => (object?)kvp.Value),
            cancellationToken: cancellationToken).ConfigureAwait(false);

        // 2. Declarar a queue do consumer com DLQ (reutiliza logica existente)
        await DeclareQueueWithDlqAsync(channel, queueName, queueOptions, cancellationToken)
            .ConfigureAwait(false);

        // 3. Bind da queue na exchange para cada pattern
        foreach (var pattern in bindingPatterns)
        {
            await channel.QueueBindAsync(
                queue: queueName,
                exchange: exchangeName,
                routingKey: pattern,
                arguments: null,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }
}
```

**Fluxo da declaracao:**

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

### 6.4 RmqPublisher — Novos Metodos

```csharp
internal sealed class RmqPublisher : IRmqPublisher
{
    // Cache de exchanges ja declaradas para evitar redeclaracoes
    private readonly HashSet<string> _declaredExchanges = new(StringComparer.Ordinal);

    // --- Metodos existentes (inalterados) ---
    // PublishAsync(queueName, payload, ...)
    // PublishAsync(queueName, payload, headers, ...)

    // --- Novos metodos ---

    public Task PublishToTopicAsync<T>(
        string exchangeName,
        string routingKey,
        T payload,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        return PublishToTopicInternalAsync(
            exchangeName, routingKey, payload,
            headers: null, cloudEventType, cancellationToken);
    }

    public Task PublishToTopicAsync<T>(
        string exchangeName,
        string routingKey,
        T payload,
        IDictionary<string, object> headers,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(headers);
        return PublishToTopicInternalAsync(
            exchangeName, routingKey, payload,
            headers, cloudEventType, cancellationToken);
    }

    private async Task PublishToTopicInternalAsync<T>(
        string exchangeName,
        string routingKey,
        T payload,
        IDictionary<string, object>? headers,
        string? cloudEventType,
        CancellationToken cancellationToken)
        where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(exchangeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        ArgumentNullException.ThrowIfNull(payload);

        await EnsureChannelAsync(cancellationToken).ConfigureAwait(false);
        await EnsureExchangeDeclaredAsync(exchangeName, cancellationToken).ConfigureAwait(false);

        var body = _cloudEventWrapper.Wrap(payload, cloudEventType);

        // Reutiliza a mesma logica de retry do publish direto
        var retryPipeline = BuildRetryPipeline(_options.DefaultRetry, _logger);

        try
        {
            await retryPipeline.ExecuteAsync(async ct =>
            {
                var properties = new BasicProperties
                {
                    ContentType = "application/cloudevents+json",
                    DeliveryMode = DeliveryModes.Persistent,
                    MessageId = Guid.NewGuid().ToString(),
                    Headers = headers is null
                        ? null
                        : headers.ToDictionary(kvp => kvp.Key, kvp => (object?)kvp.Value)
                };

                await _channel!.BasicPublishAsync(
                    exchange: exchangeName,
                    routingKey: routingKey,
                    mandatory: false,   // false: mensagem sem binding eh descartada silenciosamente
                    basicProperties: properties,
                    body: body,
                    cancellationToken: ct).ConfigureAwait(false);

                _logger.LogDebug(
                    "Mensagem publicada na exchange {Exchange} com routing key {RoutingKey}",
                    exchangeName, routingKey);
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Falha ao publicar na exchange {Exchange} com routing key {RoutingKey} apos retries",
                exchangeName, routingKey);
            throw new RmqPublishException(
                $"{exchangeName}/{routingKey}",
                _options.DefaultRetry.MaxAttempts, ex);
        }
    }

    /// <summary>
    /// Garante que a exchange topic esta declarada (idempotente).
    /// </summary>
    private async Task EnsureExchangeDeclaredAsync(string exchangeName, CancellationToken cancellationToken)
    {
        if (_declaredExchanges.Contains(exchangeName))
        {
            return;
        }

        await _channelLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_declaredExchanges.Contains(exchangeName))
            {
                return;
            }

            var exchangeOptions = _options.Exchanges.TryGetValue(exchangeName, out var opts)
                ? opts
                : new ExchangeOptions { Name = exchangeName };

            await _channel!.ExchangeDeclareAsync(
                exchange: exchangeName,
                type: ExchangeType.Topic,
                durable: exchangeOptions.Durable,
                autoDelete: exchangeOptions.AutoDelete,
                arguments: exchangeOptions.Arguments?.ToDictionary(
                    kvp => kvp.Key, kvp => (object?)kvp.Value),
                cancellationToken: cancellationToken).ConfigureAwait(false);

            _declaredExchanges.Add(exchangeName);
        }
        finally
        {
            _channelLock.Release();
        }
    }
}
```

**Decisao: `mandatory: false`**

No publish via Topic Exchange, `mandatory` eh `false` (diferente do publish direto que usa `true`). Isso ocorre porque no modelo pub/sub, eh valido publicar uma mensagem em um topico mesmo que nenhum consumer esteja inscrito naquele momento. A mensagem sera descartada silenciosamente pelo broker. Esse eh o comportamento esperado do padrao pub/sub.

### 6.5 RmqTopicConsumer — Hosted Service para Topic

```csharp
namespace Rmq.CloudEvents.Consuming;

/// <summary>
/// Hosted service para consumo de mensagens via Topic Exchange.
/// </summary>
/// <typeparam name="T">Tipo do payload consumido.</typeparam>
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

    public RmqTopicConsumer(
        IRmqConnectionManager connectionManager,
        IQueueManager queueManager,
        ICloudEventWrapper cloudEventWrapper,
        IRmqMessageHandler<T> messageHandler,
        RmqOptions options,
        TopicSubscriptionOptions subscription,
        ILogger<RmqTopicConsumer<T>>? logger = null)
    {
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _queueManager = queueManager ?? throw new ArgumentNullException(nameof(queueManager));
        _cloudEventWrapper = cloudEventWrapper ?? throw new ArgumentNullException(nameof(cloudEventWrapper));
        _messageHandler = messageHandler ?? throw new ArgumentNullException(nameof(messageHandler));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _subscription = subscription ?? throw new ArgumentNullException(nameof(subscription));
        _logger = logger ?? NullLogger<RmqTopicConsumer<T>>.Instance;
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycleLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_isStarted && _channel is { IsOpen: true })
            {
                return;
            }

            await _connectionManager.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
            var channel = await _connectionManager.CreateChannelAsync(cancellationToken).ConfigureAwait(false);

            var queueName = _subscription.QueueName
                ?? throw new InvalidOperationException("QueueName is required for durable topic consumers.");

            var exchangeOptions = _options.Exchanges.TryGetValue(_subscription.ExchangeName, out var opts)
                ? opts
                : null;

            // Declara exchange + queue + DLQ + bindings
            await _queueManager.DeclareExchangeAndBindingsAsync(
                channel,
                _subscription.ExchangeName,
                queueName,
                _subscription.BindingPatterns,
                _subscription.Queue,
                exchangeOptions,
                cancellationToken).ConfigureAwait(false);

            var retryOptions = _subscription.Queue.Retry;

            var consumerHandler = new RmqAsyncConsumerHandler<T>(
                channel,
                _messageHandler,
                _cloudEventWrapper,
                retryOptions,
                queueName,
                _logger);

            var consumerTag = await channel.BasicConsumeAsync(
                queue: queueName,
                autoAck: false,
                consumerTag: string.Empty,
                noLocal: false,
                exclusive: false,
                arguments: null,
                consumer: consumerHandler,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            _channel = channel;
            _consumerTag = consumerTag;
            _isStarted = true;

            _logger.LogInformation(
                "Topic consumer iniciado. Exchange={Exchange}, Queue={Queue}, Patterns=[{Patterns}], Tag={Tag}",
                _subscription.ExchangeName,
                queueName,
                string.Join(", ", _subscription.BindingPatterns),
                _consumerTag);
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        // Mesma logica de stop do RmqConsumer existente
        await _lifecycleLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!_isStarted || _channel is null)
            {
                return;
            }

            var channel = _channel;
            var consumerTag = _consumerTag;

            _channel = null;
            _consumerTag = null;
            _isStarted = false;

            if (!string.IsNullOrWhiteSpace(consumerTag))
            {
                await channel.BasicCancelAsync(consumerTag, false, cancellationToken)
                    .ConfigureAwait(false);
            }

            if (channel.IsOpen)
            {
                await channel.CloseAsync(200, "Topic consumer stopped", false, cancellationToken)
                    .ConfigureAwait(false);
            }

            await channel.DisposeAsync().ConfigureAwait(false);

            _logger.LogInformation(
                "Topic consumer parado. Exchange={Exchange}, Queue={Queue}",
                _subscription.ExchangeName,
                _subscription.QueueName);
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _lifecycleLock.Dispose();
    }
}
```

### 6.6 RmqAsyncConsumerHandler — Alteracao Minima

O handler ja recebe `exchange` e `routingKey` no metodo `HandleBasicDeliverAsync` via parametros do RabbitMQ.Client. A unica alteracao necessaria eh repassar esses valores para o `MessageContext`:

```csharp
// Dentro de RmqAsyncConsumerHandler<T>.HandleBasicDeliverAsync
// JA RECEBE: string exchange, string routingKey

private MessageContext CreateMessageContext(
    CloudEventMetadata metadata,
    IReadOnlyDictionary<string, object> headers,
    ulong deliveryTag,
    int currentAttempt,
    bool redelivered,
    string exchange,      // novo parametro
    string routingKey)    // novo parametro
{
    var initialAttempt = redelivered ? 2 : 1;

    return new MessageContext
    {
        EventId = metadata.EventId,
        Source = metadata.Source,
        EventType = metadata.EventType,
        Timestamp = metadata.Timestamp,
        Headers = headers,
        DeliveryTag = deliveryTag,
        QueueName = _queueName,
        AttemptNumber = (initialAttempt - 1) + currentAttempt,
        ExchangeName = exchange,      // novo
        RoutingKey = routingKey        // novo
    };
}
```

### 6.7 ServiceCollectionExtensions — Novo Metodo

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
        ExchangeName = null!,   // sera definido pelo configure
        BindingPatterns = null!  // sera definido pelo configure
    };
    configure(subscription);

    // Validacao
    ArgumentException.ThrowIfNullOrWhiteSpace(subscription.ExchangeName);
    if (subscription.BindingPatterns is null || subscription.BindingPatterns.Count == 0)
    {
        throw new ArgumentException("At least one binding pattern is required.");
    }
    if (string.IsNullOrWhiteSpace(subscription.QueueName))
    {
        throw new ArgumentException("QueueName is required for durable topic consumers.");
    }

    services.AddTransient<IRmqMessageHandler<TMessage>, THandler>();
    services.AddHostedService(sp =>
        new RmqTopicConsumer<TMessage>(
            sp.GetRequiredService<IRmqConnectionManager>(),
            sp.GetRequiredService<IQueueManager>(),
            sp.GetRequiredService<ICloudEventWrapper>(),
            sp.GetRequiredService<IRmqMessageHandler<TMessage>>(),
            sp.GetRequiredService<RmqOptions>(),
            subscription,
            sp.GetService<Microsoft.Extensions.Logging.ILogger<RmqTopicConsumer<TMessage>>>()));

    return services;
}
```

---

## 7. Fluxo de Dados Detalhado

### 7.1 Topic Publish Flow

```
Developer                       Library                              RabbitMQ
    |                               |                                    |
    |-- PublishToTopicAsync ------->|                                    |
    |   (exchange, routingKey,      |                                    |
    |    payload)                   |                                    |
    |                               |-- EnsureChannel                    |
    |                               |-- EnsureExchangeDeclared           |
    |                               |   (ExchangeDeclare topic)          |
    |                               |-- Wrap CloudEvent                  |
    |                               |                                    |
    |                               |-- [Retry Loop - Polly]             |
    |                               |   |                                |
    |                               |   |-- BasicPublishAsync ---------->|
    |                               |   |   exchange: "business-events"  |
    |                               |   |   routingKey: "orders.created" |
    |                               |   |                                |
    |                               |   |<-- Success/Failure ------------|
    |                               |                                    |
    |<-- Task completed ------------|                                    |
```

### 7.2 Topic Consumer Startup Flow

```
Application Start               Library                              RabbitMQ
    |                               |                                    |
    |-- Host.StartAsync ----------->|                                    |
    |                               |-- GetConnectionAsync               |
    |                               |-- CreateChannelAsync               |
    |                               |                                    |
    |                               |-- DeclareExchangeAndBindingsAsync  |
    |                               |   |                                |
    |                               |   |-- ExchangeDeclare ------------>|
    |                               |   |   (topic, "business-events")   |
    |                               |   |                                |
    |                               |   |-- DeclareQueueWithDlqAsync     |
    |                               |   |   (DLX + DLQ + queue)          |
    |                               |   |                                |
    |                               |   |-- QueueBind ----------------->|
    |                               |   |   ("orders.*")                 |
    |                               |   |-- QueueBind ----------------->|
    |                               |       ("payments.#")               |
    |                               |                                    |
    |                               |-- BasicConsumeAsync -------------->|
    |                               |                                    |
    |<-- Consumer running ----------|                                    |
```

### 7.3 Topic Message Delivery Flow

```
RabbitMQ                         Library                            Developer
    |                               |                                    |
    |-- BasicDeliver -------------->|                                    |
    |   exchange: "business-events" |                                    |
    |   routingKey: "orders.created"|                                    |
    |                               |                                    |
    |                               |-- Unwrap CloudEvent                |
    |                               |-- Build MessageContext             |
    |                               |   (ExchangeName, RoutingKey)       |
    |                               |                                    |
    |                               |-- [Retry Loop - Polly]             |
    |                               |   |                                |
    |                               |   |-- HandleAsync(payload) ------->|
    |                               |   |   context.RoutingKey =         |
    |                               |   |     "orders.created"           |
    |                               |   |   context.ExchangeName =       |
    |                               |   |     "business-events"          |
    |                               |   |                                |
    |                               |   |<-- Success/Exception ----------|
    |                               |                                    |
    |<-- BasicAck ------------------|  (sucesso)                         |
    |<-- BasicNack (requeue:false) -|  (falha -> DLQ)                    |
```

---

## 8. Topologia RabbitMQ Resultante

```
                            Topic Exchange
                          "business-events"
                                 |
               ┌─────────────────┼─────────────────────┐
               │                 │                      │
          orders.*          orders.created              #
               │                 │                      │
               ▼                 ▼                      ▼
    ┌──────────────────┐ ┌─────────────────┐  ┌────────────────┐
    │ order-audit-queue│ │ new-order-queue  │  │ global-audit-q │
    │    (quorum)      │ │    (quorum)      │  │   (quorum)     │
    └────────┬─────────┘ └────────┬────────┘  └───────┬────────┘
             │                    │                    │
          DLX/DLQ              DLX/DLQ              DLX/DLQ
```

---

## 9. Estrutura de Arquivos (Alteracoes)

```
src/Rmq.CloudEvents/
├── Configuration/
│   ├── CloudEventsOptions.cs        # (inalterado)
│   ├── DlqOptions.cs                # (inalterado)
│   ├── ExchangeOptions.cs           # NOVO
│   ├── QueueOptions.cs              # (inalterado)
│   ├── RetryOptions.cs              # (inalterado)
│   ├── RmqConnectionOptions.cs      # (inalterado)
│   ├── RmqOptions.cs                # ALTERADO (+ Exchanges property)
│   └── TopicSubscriptionOptions.cs  # NOVO
├── Consuming/
│   ├── IRmqConsumer.cs              # (inalterado)
│   ├── IRmqMessageHandler.cs        # (inalterado)
│   ├── MessageContext.cs            # ALTERADO (+ ExchangeName, RoutingKey)
│   ├── RmqAsyncConsumerHandler.cs   # ALTERADO (repassa exchange/routingKey)
│   ├── RmqConsumer.cs               # (inalterado)
│   └── RmqTopicConsumer.cs          # NOVO
├── Extensions/
│   └── ServiceCollectionExtensions.cs  # ALTERADO (+ AddRmqTopicConsumer)
├── Infrastructure/
│   ├── IQueueManager.cs             # ALTERADO (+ DeclareExchangeAndBindingsAsync)
│   └── QueueManager.cs              # ALTERADO (+ implementacao)
├── Publishing/
│   ├── IRmqPublisher.cs             # ALTERADO (+ PublishToTopicAsync)
│   └── RmqPublisher.cs              # ALTERADO (+ implementacao)
└── ... (demais inalterados)

tests/Rmq.CloudEvents.Tests/
├── Configuration/
│   └── ExchangeOptionsTests.cs              # NOVO
│   └── TopicSubscriptionOptionsTests.cs     # NOVO
├── Consuming/
│   └── RmqTopicConsumerTests.cs             # NOVO
├── Infrastructure/
│   └── QueueManagerExchangeTests.cs         # NOVO
├── Publishing/
│   └── RmqPublisherTopicTests.cs            # NOVO
└── Extensions/
    └── ServiceCollectionTopicExtensionsTests.cs  # NOVO

tests/Rmq.CloudEvents.IntegrationTests/
└── TopicExchangeTests.cs                    # NOVO
```

---

## 10. Estrategia de Testes

### 10.1 Testes Unitarios (Novos)

| Area | O que testar |
|---|---|
| `ExchangeOptions` | Defaults corretos (durable=true, autoDelete=false) |
| `TopicSubscriptionOptions` | Defaults corretos, validacao de patterns |
| `QueueManager.DeclareExchangeAndBindingsAsync` | Exchange declarada como `topic`, bindings criados para cada pattern, DLQ configurada, reutiliza `DeclareQueueWithDlqAsync` |
| `RmqPublisher.PublishToTopicAsync` | Exchange declarada antes do publish, `BasicPublishAsync` chamado com exchange e routing key corretos, retry funciona, `mandatory=false` |
| `RmqPublisher.PublishToTopicAsync` com headers | Headers customizados repassados ao `BasicProperties` |
| `RmqTopicConsumer.StartAsync` | Declara exchange+bindings, inicia consumer |
| `RmqTopicConsumer.StopAsync` | Cancela consumer, fecha channel |
| `RmqAsyncConsumerHandler` | `MessageContext.ExchangeName` e `RoutingKey` preenchidos |
| `AddRmqTopicConsumer` | Registra `IRmqMessageHandler` e `IHostedService`, valida parametros |
| `RmqOptions.Exchanges` | Dicionario inicializado vazio, nao afeta options existentes |
| Backward-compat | `PublishAsync(queueName, ...)` continua usando `exchange: ""` |

### 10.2 Testes de Integracao (Novos)

| Cenario | Validacao |
|---|---|
| Publish + Consume via Topic Exchange | Mensagem publicada com routing key `orders.created` eh recebida por consumer com binding `orders.*` |
| Multiplos bindings | Consumer com `["orders.*", "payments.*"]` recebe mensagens de ambos os topicos |
| Binding seletivo `#` | Consumer com `#` recebe todas as mensagens da exchange |
| Binding exato | Consumer com `orders.created` recebe apenas esse topico, nao `orders.updated` |
| DLQ via Topic Exchange | Mensagem que falha no handler eh roteada para DLQ |
| CloudEvents no wire (topic) | Mensagem na exchange contem CloudEvent valido |
| Multiplos consumers mesma exchange | Dois consumers com patterns diferentes recebem mensagens corretas |
| Coexistencia queue direta + topic | `PublishAsync` na queue e `PublishToTopicAsync` na exchange funcionam simultaneamente |

### 10.3 Meta de Cobertura

- Testes unitarios novos: >= 90% cobertura dos novos componentes.
- Todos os cenarios de integracao listados acima devem passar.

---

## 11. Analise de Breaking Changes

| Componente | Tipo de Mudanca | Breaking? | Justificativa |
|---|---|---|---|
| `IRmqPublisher` | Novos metodos | **Nao** | Metodos adicionais em interface; implementacao interna `RmqPublisher` eh `internal sealed`, consumidores usam via DI |
| `IQueueManager` | Novo metodo | **Nao** | Interface `internal`, nao exposta ao consumidor |
| `RmqOptions` | Nova propriedade | **Nao** | Propriedade com default (`new Dictionary`) — config existente nao precisa de alteracao |
| `MessageContext` | Novas propriedades | **Nao** | Propriedades com defaults (`string.Empty`) — handlers existentes nao precisam usar |
| `RmqAsyncConsumerHandler` | Alteracao interna | **Nao** | Classe `internal sealed` |
| `ServiceCollectionExtensions` | Novo metodo | **Nao** | Metodo adicional, existentes inalterados |
| `QueueManager` | Novo metodo | **Nao** | Classe `internal sealed`, novo metodo |
| `RmqPublisher` | Novos metodos + campo | **Nao** | Classe `internal sealed` |
| `ExchangeOptions` | Nova classe | **Nao** | Aditividade pura |
| `TopicSubscriptionOptions` | Nova classe | **Nao** | Aditividade pura |
| `RmqTopicConsumer<T>` | Nova classe | **Nao** | Aditividade pura |

**Conclusao: Zero breaking changes.** Todas as alteracoes sao aditivas. Nenhuma assinatura existente eh alterada ou removida. A configuracao existente continua funcionando sem qualquer modificacao.

---

## 12. Decisoes de Design e Trade-offs

| Decisao | Justificativa | Trade-off |
|---|---|---|
| Apenas `topic` exchange (nao `direct`/`fanout`/`headers`) | Topic eh o mais flexivel — reproduz `direct` com routing keys exatas e `fanout` com `#`. Simplifica a API. | Nao suporta `headers` exchange (raro). |
| `PublishToTopicAsync` como metodo separado (nao overload de `PublishAsync`) | Nome explicito evita ambiguidade. Parametros diferentes (exchange vs queue). | Duas "familias" de metodos no publisher. |
| `mandatory: false` no topic publish | Padrao pub/sub aceita mensagens sem consumers. Evita `BasicReturn` exceptions. | Mensagem descartada silenciosamente se nenhum consumer estiver inscrito. |
| `QueueName` obrigatorio em `TopicSubscriptionOptions` | Garante durabilidade e permite restart do consumer sem perda. | Nao suporta queues anonimas/temporarias (raro em producao). |
| `ExchangeOptions` sem `Type` exposto | Escopo eh Topic Exchange. Nao ha razao para permitir `direct`/`fanout` neste momento. | Se futuramente outros tipos forem necessarios, sera preciso extender. |
| `RmqTopicConsumer<T>` como classe separada (nao adaptar `RmqConsumer<T>`) | Separacao de concerns. `RmqConsumer` continua simples. Nao polui com ifs/flags. | Duplicacao parcial de lifecycle (start/stop). |
| Reutiliza `DeclareQueueWithDlqAsync` | DLQ e quorum queue sao consistentes entre consume direto e via topic. Evita duplicacao de logica. | Acoplamento entre os dois metodos do `QueueManager`. |

---

## 13. Riscos e Mitigacoes

| Risco | Probabilidade | Impacto | Mitigacao |
|---|---|---|---|
| Exchange nao declarada antes do publish | Media | Alto | `EnsureExchangeDeclaredAsync` com cache em `_declaredExchanges` |
| Binding pattern invalido | Baixa | Medio | Validacao no `AddRmqTopicConsumer`. RabbitMQ rejeita patterns invalidos. |
| Dois consumers no mesmo `TMessage`/`THandler` chave DI | Media | Alto | Documentar que para multiplos consumers do mesmo tipo, usar handler classes distintas. |
| Mensagem publicada sem nenhum consumer inscrito | Alta | Baixo | Comportamento esperado de pub/sub. `mandatory: false` evita exceptions. Documentar. |
| Exchange redeclarada com parametros diferentes | Baixa | Alto | ExchangeDeclare eh idempotente para mesmos parametros. RabbitMQ retorna erro se parametros diferirem. Documentar. |

---

## 14. Itens Fora do Escopo (Para Futuro)

- Suporte a exchanges `direct`, `fanout` e `headers`.
- Queues anonimas (auto-delete) para consumers temporarios.
- Unbind dinamico de routing keys em runtime.
- Consumer groups / exclusive consumers.
- Exchange-to-exchange bindings.
- Alternate exchanges para mensagens unroutable.
- Metricas de mensagens por routing key.
