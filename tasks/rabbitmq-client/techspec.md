# Technical Specification: RabbitMQ .NET Library

## 1. Informacoes do Documento

| Campo | Valor |
|---|---|
| **Titulo** | Tech Spec - Biblioteca .NET para RabbitMQ com Quorum Queues, Retry Exponencial, DLQ e CloudEvents |
| **Versao** | 1.0 |
| **Data** | 07 de Fevereiro de 2026 |
| **PRD de Referencia** | [docs/prd.md](./prd.md) |
| **Guia de Referencia** | [rules/guia-dotnet-libs.md](../rules/guia-dotnet-libs.md) |
| **Status** | Draft |

---

## 2. Visao Geral

Esta especificacao tecnica detalha a implementacao da biblioteca .NET para integracao com RabbitMQ utilizando quorum queues. A biblioteca abstrai complexidades de conexao, retry com backoff exponencial, dead-letter queues (DLQ) e encapsulamento transparente de payloads em formato CloudEvents.

**Foco deste documento:** apenas a implementacao .NET. A versao Java sera tratada em documento separado.

---

## 3. Stack Tecnologica

| Componente | Tecnologia | Versao Minima |
|---|---|---|
| Runtime | .NET | 8.0 (LTS) |
| Target Framework | `net8.0` | - |
| Linguagem | C# | 12+ (latest) |
| RabbitMQ Client | `RabbitMQ.Client` | 7.x |
| Resiliencia | `Polly` | 8.x |
| CloudEvents SDK | `CloudNative.CloudEvents` | 2.x |
| CloudEvents JSON | `CloudNative.CloudEvents.SystemTextJson` | 2.x |
| Logging | `Microsoft.Extensions.Logging.Abstractions` | 8.x |
| DI | `Microsoft.Extensions.DependencyInjection.Abstractions` | 8.x |
| Serializacao | `System.Text.Json` | (incluso no runtime) |
| Testes Unitarios | `xUnit` + `Moq` | latest |
| Testes de Integracao | `Testcontainers` + `Testcontainers.RabbitMq` | latest |
| RabbitMQ Server | RabbitMQ | 3.8+ (quorum queues) |

> **Nota sobre RabbitMQ.Client 7.x:** A versao 7.x do client .NET eh totalmente assincrona (APIs `*Async`). Todas as operacoes de canal, publicacao e consumo usam `Task`/`ValueTask`. A biblioteca sera construida sobre esta API moderna.

---

## 4. Estrutura da Solucao

Seguindo o guia `guia-dotnet-libs.md`, a estrutura da solucao sera:

```
dotnet-rabbimq-lib/
├── docs/
│   ├── prd.md
│   └── techspec.md
├── rules/
│   └── guia-dotnet-libs.md
├── src/
│   └── Rmq.CloudEvents/
│       ├── Rmq.CloudEvents.csproj
│       ├── Configuration/
│       │   ├── RmqOptions.cs
│       │   ├── QueueOptions.cs
│       │   ├── RetryOptions.cs
│       │   └── CloudEventsOptions.cs
│       ├── Connection/
│       │   ├── IRmqConnectionManager.cs
│       │   └── RmqConnectionManager.cs
│       ├── Infrastructure/
│       │   ├── IQueueManager.cs
│       │   └── QueueManager.cs
│       ├── CloudEvents/
│       │   ├── ICloudEventWrapper.cs
│       │   └── CloudEventWrapper.cs
│       ├── Publishing/
│       │   ├── IRmqPublisher.cs
│       │   └── RmqPublisher.cs
│       ├── Consuming/
│       │   ├── IRmqConsumer.cs
│       │   ├── RmqConsumer.cs
│       │   └── RmqAsyncConsumerHandler.cs
│       ├── Serialization/
│       │   ├── IMessageSerializer.cs
│       │   └── SystemTextJsonMessageSerializer.cs
│       ├── Exceptions/
│       │   ├── RmqPublishException.cs
│       │   ├── RmqConsumeException.cs
│       │   └── RmqConnectionException.cs
│       └── Extensions/
│           └── ServiceCollectionExtensions.cs
├── tests/
│   ├── Rmq.CloudEvents.Tests/
│   │   ├── Rmq.CloudEvents.Tests.csproj
│   │   ├── Configuration/
│   │   ├── CloudEvents/
│   │   ├── Publishing/
│   │   ├── Consuming/
│   │   └── Serialization/
│   └── Rmq.CloudEvents.IntegrationTests/
│       ├── Rmq.CloudEvents.IntegrationTests.csproj
│       ├── Fixtures/
│       │   └── RabbitMqFixture.cs
│       ├── PublishConsumeTests.cs
│       └── DlqTests.cs
├── samples/
│   └── Rmq.CloudEvents.Sample/
│       ├── Rmq.CloudEvents.Sample.csproj
│       └── Program.cs
├── Rmq.CloudEvents.sln
├── Directory.Build.props
└── .gitignore
```

---

## 5. Configuracao do Projeto

### 5.1 Directory.Build.props

Arquivo centralizado de configuracoes compartilhadas por todos os projetos:

```xml
<Project>
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
  </PropertyGroup>
</Project>
```

### 5.2 Rmq.CloudEvents.csproj

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
    <PackageId>Rmq.CloudEvents</PackageId>
    <Version>1.0.0</Version>
    <Authors>TassoGomes</Authors>
    <Description>Biblioteca .NET para RabbitMQ com quorum queues, retry exponencial, DLQ e CloudEvents transparente.</Description>
    <PackageTags>rabbitmq;cloudevents;quorum-queues;retry;dlq;messaging</PackageTags>
    <RepositoryUrl>https://github.com/tassosgomes/dotnet-rabbimq-lib</RepositoryUrl>
    <PackageLicenseExpression>MIT</PackageLicenseExpression>
    <IncludeSymbols>true</IncludeSymbols>
    <SymbolPackageFormat>snupkg</SymbolPackageFormat>
    <PublishRepositoryUrl>true</PublishRepositoryUrl>
    <GenerateDocumentationFile>true</GenerateDocumentationFile>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="RabbitMQ.Client" Version="7.*" />
    <PackageReference Include="Polly.Core" Version="8.*" />
    <PackageReference Include="CloudNative.CloudEvents" Version="2.*" />
    <PackageReference Include="CloudNative.CloudEvents.SystemTextJson" Version="2.*" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" Version="8.*" />
    <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions" Version="8.*" />
    <PackageReference Include="Microsoft.Extensions.Options" Version="8.*" />
  </ItemGroup>

</Project>
```

---

## 6. Design da API Publica

### 6.1 Principio: Scenario-Driven Design

A API foi desenhada para que os cenarios mais comuns sejam simples e diretos. O desenvolvedor **nao precisa conhecer CloudEvents** nem detalhes de quorum queues ou DLQ.

### 6.2 Cenario 1: Registro via Dependency Injection

```csharp
// Program.cs ou Startup.cs
services.AddRmqCloudEvents(options =>
{
    options.Connection = new RmqConnectionOptions
    {
        HostName = "localhost",
        Port = 5672,
        UserName = "guest",
        Password = "guest",
        VirtualHost = "/"
    };

    options.DefaultCloudEvents = new CloudEventsOptions
    {
        Source = new Uri("/my-service", UriKind.Relative),
        DefaultType = "com.mycompany.events"
    };
});
```

### 6.3 Cenario 2: Publicar uma mensagem

```csharp
public class OrderService
{
    private readonly IRmqPublisher _publisher;

    public OrderService(IRmqPublisher publisher)
    {
        _publisher = publisher;
    }

    public async Task PlaceOrderAsync(Order order, CancellationToken ct)
    {
        await _publisher.PublishAsync(
            queueName: "orders",
            payload: order,
            cancellationToken: ct);
    }
}
```

O desenvolvedor envia apenas o `Order`. Internamente, a biblioteca:
1. Serializa `order` para JSON via `System.Text.Json`.
2. Encapsula em CloudEvent com `id`, `source`, `type`, `time`, `data`.
3. Publica na quorum queue `orders` com retry exponencial.
4. Caso falhe apos todas as tentativas, envia para `orders.dlq`.

### 6.4 Cenario 3: Consumir mensagens

```csharp
public class OrderConsumer : IRmqMessageHandler<Order>
{
    public async Task HandleAsync(Order message, MessageContext context, CancellationToken ct)
    {
        // Logica de negocio - recebe apenas o payload puro
        await ProcessOrderAsync(message);
    }
}

// Registro
services.AddRmqConsumer<Order, OrderConsumer>("orders");
```

O desenvolvedor recebe apenas o `Order` deserializado. O CloudEvent eh desencapsulado internamente. O `MessageContext` fornece metadados opcionais (CloudEvent id, source, type, headers, delivery tag).

### 6.5 Cenario 4: Configuracao avancada por queue

```csharp
services.AddRmqCloudEvents(options =>
{
    options.Connection = new RmqConnectionOptions { /* ... */ };

    options.Queues.Add("orders", new QueueOptions
    {
        QuorumSize = 3,
        DeliveryLimit = 5,
        Retry = new RetryOptions
        {
            MaxAttempts = 5,
            InitialDelay = TimeSpan.FromSeconds(1),
            BackoffType = BackoffType.Exponential,
            UseJitter = true
        },
        Dlq = new DlqOptions
        {
            Enabled = true,
            QueueNameSuffix = ".dlq"
        }
    });
});
```

---

## 7. Contratos (Interfaces Publicas)

### 7.1 IRmqPublisher

```csharp
namespace Rmq.CloudEvents.Publishing;

/// <summary>
/// Publica mensagens em quorum queues do RabbitMQ com retry e CloudEvents transparentes.
/// </summary>
public interface IRmqPublisher : IAsyncDisposable
{
    /// <summary>
    /// Publica um payload na queue especificada.
    /// O payload eh automaticamente encapsulado em CloudEvent.
    /// </summary>
    /// <typeparam name="T">Tipo do payload.</typeparam>
    /// <param name="queueName">Nome da queue destino.</param>
    /// <param name="payload">Payload a ser publicado.</param>
    /// <param name="cloudEventType">Tipo do CloudEvent (opcional, usa default se nao informado).</param>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    Task PublishAsync<T>(
        string queueName,
        T payload,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Publica um payload com headers customizados.
    /// </summary>
    Task PublishAsync<T>(
        string queueName,
        T payload,
        IDictionary<string, object> headers,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default) where T : class;
}
```

### 7.2 IRmqConsumer

```csharp
namespace Rmq.CloudEvents.Consuming;

/// <summary>
/// Gerencia o consumo de mensagens de quorum queues do RabbitMQ.
/// </summary>
public interface IRmqConsumer : IAsyncDisposable
{
    /// <summary>
    /// Inicia o consumo de mensagens da queue especificada.
    /// </summary>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Para o consumo de mensagens.
    /// </summary>
    Task StopAsync(CancellationToken cancellationToken = default);
}
```

### 7.3 IRmqMessageHandler<T>

```csharp
namespace Rmq.CloudEvents.Consuming;

/// <summary>
/// Handler de mensagens implementado pelo desenvolvedor.
/// Recebe apenas o payload deserializado, sem expor CloudEvents.
/// </summary>
/// <typeparam name="T">Tipo do payload.</typeparam>
public interface IRmqMessageHandler<in T> where T : class
{
    /// <summary>
    /// Processa uma mensagem recebida.
    /// </summary>
    /// <param name="message">Payload deserializado.</param>
    /// <param name="context">Metadados da mensagem (opcionais).</param>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    Task HandleAsync(T message, MessageContext context, CancellationToken cancellationToken);
}
```

### 7.4 MessageContext

```csharp
namespace Rmq.CloudEvents.Consuming;

/// <summary>
/// Contexto da mensagem recebida. Fornece metadados opcionais sem expor CloudEvents diretamente.
/// </summary>
public sealed class MessageContext
{
    /// <summary>ID unico do evento (CloudEvent id).</summary>
    public required string EventId { get; init; }

    /// <summary>Source do evento (CloudEvent source).</summary>
    public required Uri Source { get; init; }

    /// <summary>Tipo do evento (CloudEvent type).</summary>
    public required string EventType { get; init; }

    /// <summary>Timestamp do evento (CloudEvent time).</summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>Headers customizados da mensagem.</summary>
    public IReadOnlyDictionary<string, object> Headers { get; init; } = new Dictionary<string, object>();

    /// <summary>Delivery tag do RabbitMQ (para controle avancado).</summary>
    public ulong DeliveryTag { get; init; }

    /// <summary>Nome da queue de origem.</summary>
    public required string QueueName { get; init; }

    /// <summary>Numero da tentativa atual de processamento.</summary>
    public int AttemptNumber { get; init; }
}
```

### 7.5 IRmqConnectionManager (interno, exposto via interface para teste)

```csharp
namespace Rmq.CloudEvents.Connection;

/// <summary>
/// Gerencia conexoes e canais com o RabbitMQ.
/// </summary>
internal interface IRmqConnectionManager : IAsyncDisposable
{
    /// <summary>Obtem ou cria uma conexao ativa.</summary>
    Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);

    /// <summary>Cria um novo canal.</summary>
    Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default);
}
```

### 7.6 ICloudEventWrapper (interno)

```csharp
namespace Rmq.CloudEvents.CloudEvents;

/// <summary>
/// Encapsula e desencapsula payloads em CloudEvents. Uso interno.
/// </summary>
internal interface ICloudEventWrapper
{
    /// <summary>Encapsula um payload serializado em um CloudEvent.</summary>
    ReadOnlyMemory<byte> Wrap<T>(T payload, string? eventType = null) where T : class;

    /// <summary>Desencapsula um CloudEvent e retorna o payload deserializado.</summary>
    (T Payload, CloudEventMetadata Metadata) Unwrap<T>(ReadOnlyMemory<byte> data) where T : class;
}
```

---

## 8. Componentes Internos e Detalhamento

### 8.1 RmqConnectionManager

**Responsabilidade:** Gerenciar o ciclo de vida da conexao com RabbitMQ.

**Implementacao:**
- Usa `RabbitMQ.Client.ConnectionFactory` com:
  - `AutomaticRecoveryEnabled = true`
  - `TopologyRecoveryEnabled = true`
  - `NetworkRecoveryInterval = TimeSpan.FromSeconds(10)`
- Singleton lifecycle (uma conexao por instancia da aplicacao).
- Criacao de canais sob demanda via `CreateChannelAsync`.
- Suporte a SSL/TLS configuravel via `RmqConnectionOptions.Ssl`.

```csharp
internal sealed class RmqConnectionManager : IRmqConnectionManager
{
    private readonly RmqConnectionOptions _options;
    private readonly ILogger<RmqConnectionManager> _logger;
    private IConnection? _connection;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    public async Task<IConnection> GetConnectionAsync(CancellationToken ct)
    {
        if (_connection is { IsOpen: true })
            return _connection;

        await _connectionLock.WaitAsync(ct);
        try
        {
            if (_connection is { IsOpen: true })
                return _connection;

            var factory = new ConnectionFactory
            {
                HostName = _options.HostName,
                Port = _options.Port,
                UserName = _options.UserName,
                Password = _options.Password,
                VirtualHost = _options.VirtualHost,
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
            };

            if (_options.Ssl is not null)
            {
                factory.Ssl = _options.Ssl;
            }

            _connection = await factory.CreateConnectionAsync(ct);
            _logger.LogInformation("Conexao RabbitMQ estabelecida em {Host}:{Port}", _options.HostName, _options.Port);
            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    public async Task<IChannel> CreateChannelAsync(CancellationToken ct)
    {
        var connection = await GetConnectionAsync(ct);
        return await connection.CreateChannelAsync(cancellationToken: ct);
    }
}
```

### 8.2 QueueManager

**Responsabilidade:** Declarar quorum queues e suas DLQs associadas.

**Implementacao:**
- Declara a queue principal como quorum queue (`x-queue-type: quorum`).
- Declara automaticamente a DLQ (`{queueName}.dlq`) como quorum queue.
- Configura Dead Letter Exchange (DLX) na queue principal apontando para a DLQ.
- Usa exchange do tipo `direct` para roteamento.

```csharp
internal sealed class QueueManager : IQueueManager
{
    public async Task DeclareQueueWithDlqAsync(
        IChannel channel,
        string queueName,
        QueueOptions options,
        CancellationToken ct)
    {
        var dlqName = $"{queueName}{options.Dlq.QueueNameSuffix}";
        var dlxName = $"{queueName}.dlx";

        // 1. Declarar DLX (exchange para DLQ)
        await channel.ExchangeDeclareAsync(
            exchange: dlxName,
            type: "direct",
            durable: true,
            autoDelete: false,
            arguments: null,
            cancellationToken: ct);

        // 2. Declarar DLQ como quorum queue
        var dlqArgs = new Dictionary<string, object?>
        {
            ["x-queue-type"] = "quorum"
        };

        await channel.QueueDeclareAsync(
            queue: dlqName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: dlqArgs,
            cancellationToken: ct);

        // 3. Bind DLQ ao DLX
        await channel.QueueBindAsync(
            queue: dlqName,
            exchange: dlxName,
            routingKey: queueName,
            arguments: null,
            cancellationToken: ct);

        // 4. Declarar queue principal como quorum com DLX
        var queueArgs = new Dictionary<string, object?>
        {
            ["x-queue-type"] = "quorum",
            ["x-dead-letter-exchange"] = dlxName,
            ["x-dead-letter-routing-key"] = queueName,
            ["x-delivery-limit"] = options.DeliveryLimit
        };

        if (options.QuorumSize > 0)
        {
            queueArgs["x-quorum-initial-group-size"] = options.QuorumSize;
        }

        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArgs,
            cancellationToken: ct);
    }
}
```

### 8.3 CloudEventWrapper

**Responsabilidade:** Encapsular/desencapsular payloads em CloudEvents de forma transparente.

**Implementacao:**
- Usa `CloudNative.CloudEvents` + `CloudNative.CloudEvents.SystemTextJson`.
- Modo estruturado JSON (structured content mode).
- Atributos CloudEvent obrigatorios preenchidos automaticamente:
  - `id`: `Guid.NewGuid().ToString()`
  - `source`: configurado via `CloudEventsOptions.Source`
  - `type`: configurado via parametro ou `CloudEventsOptions.DefaultType`
  - `time`: `DateTimeOffset.UtcNow`
  - `datacontenttype`: `application/json`
- Serializa o payload para `data` usando `System.Text.Json`.

```csharp
internal sealed class CloudEventWrapper : ICloudEventWrapper
{
    private readonly CloudEventsOptions _options;
    private readonly JsonEventFormatter _formatter;
    private readonly JsonSerializerOptions _jsonOptions;

    public ReadOnlyMemory<byte> Wrap<T>(T payload, string? eventType = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(payload);

        var cloudEvent = new CloudEvent
        {
            Id = Guid.NewGuid().ToString(),
            Source = _options.Source,
            Type = eventType ?? _options.DefaultType,
            Time = DateTimeOffset.UtcNow,
            DataContentType = "application/json",
            Data = payload
        };

        var bytes = _formatter.EncodeStructuredModeMessage(cloudEvent, out _);
        return bytes;
    }

    public (T Payload, CloudEventMetadata Metadata) Unwrap<T>(ReadOnlyMemory<byte> data) where T : class
    {
        var cloudEvent = _formatter.DecodeStructuredModeMessage(data, contentType: null, extensionAttributes: null);

        var payload = cloudEvent.Data switch
        {
            T typed => typed,
            JsonElement jsonElement => jsonElement.Deserialize<T>(_jsonOptions)
                ?? throw new RmqConsumeException($"Falha ao deserializar payload do tipo {typeof(T).Name}"),
            _ => throw new RmqConsumeException(
                $"Tipo de data inesperado: {cloudEvent.Data?.GetType().Name ?? "null"}")
        };

        var metadata = new CloudEventMetadata
        {
            EventId = cloudEvent.Id ?? string.Empty,
            Source = cloudEvent.Source ?? _options.Source,
            EventType = cloudEvent.Type ?? _options.DefaultType,
            Timestamp = cloudEvent.Time ?? DateTimeOffset.UtcNow
        };

        return (payload, metadata);
    }
}
```

### 8.4 RmqPublisher

**Responsabilidade:** Publicar mensagens com retry exponencial e encapsulamento CloudEvent.

**Implementacao:**
- Usa `Polly.Core` (`ResiliencePipeline`) para retry com backoff exponencial.
- Configuracao padrao: 5 tentativas, delays de ~1s, 2s, 4s, 8s, 16s (com jitter).
- Apos esgotar retries, a excecao eh propagada. O RabbitMQ roteara para DLQ via DLX se o `x-delivery-limit` for atingido.
- Publisher confirms ativados para garantia de entrega.

```csharp
internal sealed class RmqPublisher : IRmqPublisher
{
    private readonly IRmqConnectionManager _connectionManager;
    private readonly IQueueManager _queueManager;
    private readonly ICloudEventWrapper _cloudEventWrapper;
    private readonly ResiliencePipeline _retryPipeline;
    private readonly ILogger<RmqPublisher> _logger;
    private IChannel? _channel;

    public async Task PublishAsync<T>(
        string queueName,
        T payload,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        ArgumentNullException.ThrowIfNull(payload);

        await EnsureChannelAsync(cancellationToken);
        await _queueManager.DeclareQueueWithDlqAsync(_channel!, queueName, GetQueueOptions(queueName), cancellationToken);

        var body = _cloudEventWrapper.Wrap(payload, cloudEventType);

        await _retryPipeline.ExecuteAsync(async ct =>
        {
            var properties = new BasicProperties
            {
                ContentType = "application/cloudevents+json",
                DeliveryMode = DeliveryModes.Persistent,
                MessageId = Guid.NewGuid().ToString()
            };

            await _channel!.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queueName,
                mandatory: true,
                basicProperties: properties,
                body: body,
                cancellationToken: ct);

            _logger.LogDebug("Mensagem publicada na queue {QueueName}", queueName);
        }, cancellationToken);
    }
}
```

**Configuracao do ResiliencePipeline para publish:**

```csharp
private static ResiliencePipeline BuildRetryPipeline(RetryOptions options, ILogger logger)
{
    return new ResiliencePipelineBuilder()
        .AddRetry(new RetryStrategyOptions
        {
            ShouldHandle = new PredicateBuilder()
                .Handle<RabbitMQClientException>()
                .Handle<IOException>()
                .Handle<TimeoutException>(),
            MaxRetryAttempts = options.MaxAttempts,
            Delay = options.InitialDelay,
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = options.UseJitter,
            OnRetry = args =>
            {
                logger.LogWarning(
                    args.Outcome.Exception,
                    "Tentativa de publish {Attempt}/{Max} falhou. Proximo retry em {Delay}",
                    args.AttemptNumber + 1,
                    options.MaxAttempts,
                    args.RetryDelay);
                return default;
            }
        })
        .Build();
}
```

### 8.5 RmqConsumer e RmqAsyncConsumerHandler

**Responsabilidade:** Consumir mensagens com desencapsulamento CloudEvent e retry no processamento.

**Implementacao:**
- Implementa `IAsyncBasicConsumer` do RabbitMQ.Client 7.x.
- Para cada mensagem recebida:
  1. Desencapsula o CloudEvent via `ICloudEventWrapper.Unwrap<T>()`.
  2. Monta o `MessageContext` com metadados.
  3. Invoca o `IRmqMessageHandler<T>.HandleAsync()` do desenvolvedor.
  4. Em caso de sucesso: `BasicAckAsync`.
  5. Em caso de falha: retry com Polly. Apos esgotar retries: `BasicNackAsync(requeue: false)` para enviar a DLQ.

```csharp
internal sealed class RmqAsyncConsumerHandler<T> : AsyncDefaultBasicConsumer where T : class
{
    private readonly IRmqMessageHandler<T> _handler;
    private readonly ICloudEventWrapper _cloudEventWrapper;
    private readonly ResiliencePipeline _retryPipeline;
    private readonly string _queueName;
    private readonly ILogger _logger;

    public override async Task HandleBasicDeliverAsync(
        string consumerTag,
        ulong deliveryTag,
        bool redelivered,
        string exchange,
        string routingKey,
        IReadOnlyBasicProperties properties,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var (payload, metadata) = _cloudEventWrapper.Unwrap<T>(body);

            var context = new MessageContext
            {
                EventId = metadata.EventId,
                Source = metadata.Source,
                EventType = metadata.EventType,
                Timestamp = metadata.Timestamp,
                QueueName = _queueName,
                DeliveryTag = deliveryTag,
                AttemptNumber = redelivered ? 1 : 0
            };

            await _retryPipeline.ExecuteAsync(async ct =>
            {
                await _handler.HandleAsync(payload, context, ct);
            }, cancellationToken);

            await Channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken);
            _logger.LogDebug("Mensagem {EventId} processada com sucesso da queue {Queue}", metadata.EventId, _queueName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Mensagem {DeliveryTag} falhou apos todos os retries na queue {Queue}. Enviando para DLQ.",
                deliveryTag, _queueName);

            await Channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false, cancellationToken);
        }
    }
}
```

### 8.6 Serialization

```csharp
namespace Rmq.CloudEvents.Serialization;

internal interface IMessageSerializer
{
    byte[] Serialize<T>(T value) where T : class;
    T Deserialize<T>(ReadOnlySpan<byte> data) where T : class;
}

internal sealed class SystemTextJsonMessageSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    public SystemTextJsonMessageSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };
    }

    public byte[] Serialize<T>(T value) where T : class
        => JsonSerializer.SerializeToUtf8Bytes(value, _options);

    public T Deserialize<T>(ReadOnlySpan<byte> data) where T : class
        => JsonSerializer.Deserialize<T>(data, _options)
           ?? throw new RmqConsumeException($"Falha ao deserializar para {typeof(T).Name}");
}
```

---

## 9. Modelos de Configuracao

### 9.1 RmqOptions (raiz)

```csharp
public sealed class RmqOptions
{
    /// <summary>Configuracoes de conexao com RabbitMQ.</summary>
    public RmqConnectionOptions Connection { get; set; } = new();

    /// <summary>Configuracoes de CloudEvents (defaults globais).</summary>
    public CloudEventsOptions DefaultCloudEvents { get; set; } = new();

    /// <summary>Configuracoes de retry padrao (aplicavel a todas as queues sem override).</summary>
    public RetryOptions DefaultRetry { get; set; } = new();

    /// <summary>Configuracoes especificas por queue.</summary>
    public Dictionary<string, QueueOptions> Queues { get; set; } = new();
}
```

### 9.2 RmqConnectionOptions

```csharp
public sealed class RmqConnectionOptions
{
    public string HostName { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string UserName { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public string VirtualHost { get; set; } = "/";
    public SslOption? Ssl { get; set; }
    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(10);
}
```

### 9.3 QueueOptions

```csharp
public sealed class QueueOptions
{
    /// <summary>Tamanho inicial do grupo quorum (0 = default do RabbitMQ).</summary>
    public int QuorumSize { get; set; } = 0;

    /// <summary>Limite de entregas antes de enviar para DLQ.</summary>
    public int DeliveryLimit { get; set; } = 5;

    /// <summary>Configuracoes de retry.</summary>
    public RetryOptions Retry { get; set; } = new();

    /// <summary>Configuracoes de DLQ.</summary>
    public DlqOptions Dlq { get; set; } = new();
}
```

### 9.4 RetryOptions

```csharp
public sealed class RetryOptions
{
    /// <summary>Numero maximo de tentativas (default: 5).</summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>Delay inicial entre retries (default: 1 segundo).</summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Tipo de backoff (default: Exponential).</summary>
    public BackoffType BackoffType { get; set; } = BackoffType.Exponential;

    /// <summary>Adicionar jitter aos delays (default: true).</summary>
    public bool UseJitter { get; set; } = true;
}

public enum BackoffType
{
    Exponential,
    Linear,
    Constant
}
```

### 9.5 DlqOptions

```csharp
public sealed class DlqOptions
{
    /// <summary>Habilitar DLQ automatica (default: true).</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Sufixo do nome da DLQ (default: ".dlq").</summary>
    public string QueueNameSuffix { get; set; } = ".dlq";
}
```

### 9.6 CloudEventsOptions

```csharp
public sealed class CloudEventsOptions
{
    /// <summary>URI de origem dos eventos (CloudEvent source). Obrigatorio.</summary>
    public Uri Source { get; set; } = new Uri("/undefined", UriKind.Relative);

    /// <summary>Tipo padrao dos eventos (CloudEvent type).</summary>
    public string DefaultType { get; set; } = "com.default.event.v1";

    /// <summary>Versao do spec CloudEvents (default: "1.0").</summary>
    public string SpecVersion { get; set; } = "1.0";
}
```

---

## 10. Excecoes Customizadas

```csharp
namespace Rmq.CloudEvents.Exceptions;

/// <summary>Excecao base da biblioteca.</summary>
public class RmqCloudEventsException : Exception
{
    public RmqCloudEventsException(string message) : base(message) { }
    public RmqCloudEventsException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>Falha ao publicar mensagem apos todos os retries.</summary>
public sealed class RmqPublishException : RmqCloudEventsException
{
    public string QueueName { get; }
    public int AttemptsExhausted { get; }

    public RmqPublishException(string queueName, int attempts, Exception inner)
        : base($"Falha ao publicar na queue '{queueName}' apos {attempts} tentativas.", inner)
    {
        QueueName = queueName;
        AttemptsExhausted = attempts;
    }
}

/// <summary>Falha ao consumir/processar mensagem.</summary>
public sealed class RmqConsumeException : RmqCloudEventsException
{
    public RmqConsumeException(string message) : base(message) { }
    public RmqConsumeException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>Falha na conexao com RabbitMQ.</summary>
public sealed class RmqConnectionException : RmqCloudEventsException
{
    public RmqConnectionException(string message) : base(message) { }
    public RmqConnectionException(string message, Exception inner) : base(message, inner) { }
}
```

---

## 11. Registro via Dependency Injection

```csharp
namespace Rmq.CloudEvents.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registra os servicos da biblioteca Rmq.CloudEvents no container de DI.
    /// </summary>
    public static IServiceCollection AddRmqCloudEvents(
        this IServiceCollection services,
        Action<RmqOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);

        // Singleton: uma conexao por aplicacao
        services.AddSingleton<IRmqConnectionManager, RmqConnectionManager>();
        services.AddSingleton<IQueueManager, QueueManager>();
        services.AddSingleton<ICloudEventWrapper, CloudEventWrapper>();
        services.AddSingleton<IMessageSerializer, SystemTextJsonMessageSerializer>();

        // Transient: publisher por uso (canais sao leves)
        services.AddTransient<IRmqPublisher, RmqPublisher>();

        return services;
    }

    /// <summary>
    /// Registra um consumer para uma queue especifica.
    /// </summary>
    public static IServiceCollection AddRmqConsumer<TMessage, THandler>(
        this IServiceCollection services,
        string queueName)
        where TMessage : class
        where THandler : class, IRmqMessageHandler<TMessage>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        services.AddTransient<IRmqMessageHandler<TMessage>, THandler>();
        services.AddHostedService(sp =>
        {
            var connectionManager = sp.GetRequiredService<IRmqConnectionManager>();
            var queueManager = sp.GetRequiredService<IQueueManager>();
            var cloudEventWrapper = sp.GetRequiredService<ICloudEventWrapper>();
            var handler = sp.GetRequiredService<IRmqMessageHandler<TMessage>>();
            var options = sp.GetRequiredService<IOptions<RmqOptions>>();
            var logger = sp.GetRequiredService<ILogger<RmqConsumer<TMessage>>>();

            return new RmqConsumer<TMessage>(
                connectionManager, queueManager, cloudEventWrapper,
                handler, options, queueName, logger);
        });

        return services;
    }
}
```

---

## 12. Fluxo de Dados Detalhado

### 12.1 Publish Flow

```
Developer                    Library                           RabbitMQ
    |                            |                                 |
    |-- PublishAsync(payload) -->|                                 |
    |                            |-- Serialize payload (STJ)       |
    |                            |-- Wrap in CloudEvent            |
    |                            |-- Encode structured JSON        |
    |                            |                                 |
    |                            |-- [Retry Loop - Polly]          |
    |                            |   |                             |
    |                            |   |-- BasicPublishAsync ------->|
    |                            |   |                             |
    |                            |   |<-- Success/Failure ---------|
    |                            |   |                             |
    |                            |   |-- (retry if failure)        |
    |                            |   |   delay: 1s, 2s, 4s,       |
    |                            |   |          8s, 16s (+jitter)  |
    |                            |                                 |
    |<-- Task completed ---------|                                 |
    |    (or RmqPublishException)|                                 |
```

### 12.2 Consume Flow

```
RabbitMQ                     Library                           Developer
    |                            |                                 |
    |-- Deliver message -------->|                                 |
    |                            |-- Decode structured JSON        |
    |                            |-- Unwrap CloudEvent             |
    |                            |-- Deserialize payload (STJ)     |
    |                            |-- Build MessageContext           |
    |                            |                                 |
    |                            |-- [Retry Loop - Polly]          |
    |                            |   |                             |
    |                            |   |-- HandleAsync(payload) ---->|
    |                            |   |                             |
    |                            |   |<-- Success/Exception -------|
    |                            |   |                             |
    |                            |   |-- (retry if exception)      |
    |                            |                                 |
    |<-- BasicAckAsync ----------|  (se sucesso)                   |
    |                            |                                 |
    |<-- BasicNackAsync ---------|  (se falha apos retries,        |
    |    (requeue: false)        |   roteado para DLQ via DLX)     |
```

### 12.3 DLQ Flow

```
Queue Principal             DLX (Exchange)                    DLQ
    |                            |                                 |
    |-- msg NACK requeue=false ->|                                 |
    |   (ou delivery-limit)      |                                 |
    |                            |-- route to DLQ --------------->|
    |                            |   (routing key = queueName)     |
    |                            |                                 |
    |                            |           Mensagem preserva     |
    |                            |           CloudEvents wrapper   |
    |                            |           + headers de erro     |
```

---

## 13. Formato CloudEvent Utilizado

A biblioteca usa **structured content mode** com JSON. Exemplo de mensagem no RabbitMQ:

```json
{
  "specversion": "1.0",
  "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "source": "/my-service",
  "type": "com.mycompany.order.created.v1",
  "time": "2026-02-07T14:30:00Z",
  "datacontenttype": "application/json",
  "data": {
    "orderId": 12345,
    "customerId": "cust-001",
    "total": 99.90
  }
}
```

**Content-Type** da mensagem AMQP: `application/cloudevents+json`

---

## 14. Thread-Safety e Lifecycle

| Componente | Lifecycle | Thread-Safe | Notas |
|---|---|---|---|
| `RmqConnectionManager` | Singleton | Sim | Protegido por SemaphoreSlim |
| `QueueManager` | Singleton | Sim | Stateless |
| `CloudEventWrapper` | Singleton | Sim | Stateless (JsonEventFormatter eh thread-safe) |
| `RmqPublisher` | Transient | Nao* | Cada instancia usa seu proprio canal. *Thread-safe para chamadas sequenciais. |
| `RmqConsumer<T>` | Hosted Service | Sim | Um por queue registrada |
| `MessageSerializer` | Singleton | Sim | Stateless |

---

## 15. Logging

A biblioteca usa `Microsoft.Extensions.Logging.Abstractions` para nao forcar nenhum provider.

**Niveis de log utilizados:**

| Nivel | Uso |
|---|---|
| `Debug` | Mensagem publicada/consumida com sucesso, detalhes de CloudEvent |
| `Information` | Conexao estabelecida, queue declarada, consumer iniciado/parado |
| `Warning` | Retry em andamento (tentativa N de M), reconexao |
| `Error` | Falha apos todos os retries, mensagem enviada para DLQ, erro de desserializacao |
| `Critical` | Falha irrecuperavel de conexao |

---

## 16. Estrategia de Testes

### 16.1 Testes Unitarios (`Rmq.CloudEvents.Tests`)

| Area | O que testar |
|---|---|
| `CloudEventWrapper` | Wrap/Unwrap com diversos tipos, campos obrigatorios presentes, roundtrip fidelidade |
| `RetryPipeline` | Numero correto de retries, delays exponenciais, jitter aplicado |
| `QueueManager` | Argumentos corretos passados ao `QueueDeclareAsync`, nomenclatura DLQ |
| `RmqPublisher` | Chamada correta ao `BasicPublishAsync`, content-type correto, retry acionado em falha |
| `RmqConsumer` | ACK em sucesso, NACK em falha, desencapsulamento correto, retry no handler |
| `MessageSerializer` | Serialize/Deserialize roundtrip, tratamento de null, tipos complexos |
| `Configuration` | Validacao de options, defaults corretos |
| `Exceptions` | Mensagens de erro claras, propriedades preenchidas |

**Ferramentas:** xUnit, Moq (para mock de `IChannel`, `IConnection`, etc.), FluentAssertions.

### 16.2 Testes de Integracao (`Rmq.CloudEvents.IntegrationTests`)

Usam **Testcontainers** para levantar um RabbitMQ real.

| Cenario | Validacao |
|---|---|
| Publish + Consume roundtrip | Mensagem publicada eh recebida com payload intacto |
| CloudEvents no wire | Mensagem no RabbitMQ contem CloudEvent valido |
| Retry no publish | Simular falha de rede, verificar retries |
| DLQ routing | Mensagem que falha N vezes vai para DLQ |
| DLQ preserva CloudEvent | Mensagem na DLQ mantem formato CloudEvent |
| Multiplas queues | Consumers em queues diferentes funcionam independentemente |
| Reconexao automatica | Matar conexao, verificar recovery |

```csharp
// Exemplo de fixture
public sealed class RabbitMqFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder()
        .WithImage("rabbitmq:3.13-management")
        .Build();

    public string ConnectionString => _container.GetConnectionString();

    public async Task InitializeAsync() => await _container.StartAsync();
    public async Task DisposeAsync() => await _container.DisposeAsync();
}
```

### 16.3 Meta de Cobertura

- **Testes unitarios:** >= 90% de cobertura de linhas nos componentes core.
- **Testes de integracao:** todos os cenarios criticos listados acima.

---

## 17. CI/CD

Pipeline recomendada (GitHub Actions):

```yaml
# .github/workflows/ci.yml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  build-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '8.0.x'

      - name: Restore
        run: dotnet restore

      - name: Build
        run: dotnet build --no-restore -c Release

      - name: Unit Tests
        run: dotnet test tests/Rmq.CloudEvents.Tests --no-build -c Release --collect:"XPlat Code Coverage"

      - name: Integration Tests
        run: dotnet test tests/Rmq.CloudEvents.IntegrationTests --no-build -c Release

      - name: Pack
        if: github.ref == 'refs/heads/main'
        run: dotnet pack src/Rmq.CloudEvents -c Release --no-build -o ./artifacts
```

---

## 18. Decisoes de Design e Trade-offs

| Decisao | Justificativa | Trade-off |
|---|---|---|
| `net8.0` apenas (sem `netstandard2.0`) | Biblioteca nova, sem necessidade de .NET Framework. Aproveita APIs modernas. | Nao suporta .NET Framework. |
| `System.Text.Json` (nao Newtonsoft) | Incluso no runtime, melhor performance, menos dependencias. | Menos features que Newtonsoft para cenarios edge. |
| Structured content mode CloudEvents | Payload autocontido, facil debug e inspecao. | Ligeiramente maior que binary mode. |
| Polly 8.x (`ResiliencePipeline`) | API moderna, melhor performance, suporte nativo a DI. | API diferente do Polly 7.x (breaking change). |
| RabbitMQ.Client 7.x | Totalmente async, melhor performance, APIs modernas. | Nao retrocompativel com projetos usando 6.x. |
| Publisher por Transient, Connection por Singleton | Canais sao leves e nao devem ser compartilhados entre threads. Conexoes sao pesadas e devem ser reutilizadas. | Publisher precisa ser obtido via DI a cada uso. |
| DLQ como quorum queue | Mesma garantia de durabilidade da queue principal. | Consome mais recursos no cluster. |
| Retry no consume eh in-process (Polly) | Rapido para falhas transitorias do handler. | Nao substitui o `x-delivery-limit` do RabbitMQ para redeliveries. Ambos atuam em camadas diferentes. |

---

## 19. Riscos e Mitigacoes

| Risco | Probabilidade | Impacto | Mitigacao |
|---|---|---|---|
| Overhead de serializacao CloudEvents | Media | Baixo | Benchmark em testes de integracao. Opcao futura de binary mode. |
| Incompatibilidade de versao RabbitMQ.Client | Baixa | Alto | Fixar major version (`7.*`), testar em CI. |
| Memory pressure em alto throughput | Media | Medio | Usar `ReadOnlyMemory<byte>` e evitar alocacoes desnecessarias. Benchmark. |
| Consumer handler bloqueante | Alta | Alto | Documentar que handlers devem ser async. Timeout configuravel futuro. |
| Perda de mensagem em crash entre consume e ACK | Baixa | Alto | At-least-once delivery garantido por quorum queues + manual ACK. |

---

## 20. Itens Fora do Escopo (Para Futuro)

- Suporte a `netstandard2.0` / multi-target.
- Binary content mode para CloudEvents (otimizacao de performance).
- Batch publishing (publicar N mensagens de uma vez).
- Circuit breaker (Polly) para protecao contra falhas prolongadas do broker.
- Metricas (OpenTelemetry / Prometheus).
- Consumer com prefetch configuravel.
- Suporte a exchange types alem de `direct`.
- Reprocessamento automatico de DLQ.
- Health checks (`IHealthCheck` do ASP.NET).

---

## 21. Glossario

| Termo | Definicao |
|---|---|
| **Quorum Queue** | Tipo de queue do RabbitMQ baseada no algoritmo Raft, replicada entre nos do cluster para alta disponibilidade. |
| **DLQ (Dead-Letter Queue)** | Queue para onde mensagens que nao puderam ser processadas sao roteadas. |
| **DLX (Dead-Letter Exchange)** | Exchange configurada para rotear mensagens rejeitadas ou expiradas para uma DLQ. |
| **CloudEvent** | Especificacao CNCF para descrever dados de eventos de forma padronizada e interoperavel. |
| **Structured Content Mode** | Modo CloudEvents onde todos os atributos e o payload sao serializados juntos em um unico documento JSON. |
| **Exponential Backoff** | Estrategia de retry onde o intervalo entre tentativas cresce exponencialmente (1s, 2s, 4s, 8s, 16s). |
| **Jitter** | Variacao aleatoria adicionada ao delay de retry para evitar "thundering herd" (multiplos clientes retentando simultaneamente). |
| **Publisher Confirms** | Mecanismo do RabbitMQ onde o broker confirma (ACK) que recebeu a mensagem publicada. |
