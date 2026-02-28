# Rmq.CloudEvents

[![CI](https://github.com/tassosgomes/dotnet-rabbimq-lib/actions/workflows/ci.yml/badge.svg)](https://github.com/tassosgomes/dotnet-rabbimq-lib/actions/workflows/ci.yml)
[![Publish NuGet](https://github.com/tassosgomes/dotnet-rabbimq-lib/actions/workflows/publish-nuget.yml/badge.svg)](https://github.com/tassosgomes/dotnet-rabbimq-lib/actions/workflows/publish-nuget.yml)
[![NuGet Version](https://img.shields.io/nuget/v/Rmq.CloudEvents)](https://www.nuget.org/packages/Rmq.CloudEvents)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Rmq.CloudEvents)](https://www.nuget.org/packages/Rmq.CloudEvents)

.NET 8 library for RabbitMQ publishing/consuming with quorum queues, exponential retry, DLQ, and transparent CloudEvents wrapping.

[Leia em Portugues (pt-BR)](README.pt-BR.md)

## Features

- Quorum queue declaration with automatic DLQ (`<queue>.dlq`) and DLX wiring.
- Transparent CloudEvents structured JSON wrapping on publish and unwrapping on consume.
- Exponential retry with Polly for publish and consumer handler execution.
- DI-first registration for ASP.NET Core / Worker services.
- Consumer pipeline with automatic ACK on success and NACK (`requeue: false`) on final failure.
- Topic exchange pub/sub support with routing keys and wildcard bindings (`*`, `#`).
- New DI extension `AddRmqTopicConsumer<TMessage, THandler>(Action<TopicSubscriptionOptions>)`.
- Built-in diagnostics with `ActivitySource` and `Meter` for tracing and metrics.
- Automatic consumer session recovery after channel shutdown or callback exceptions.

## Requirements

- .NET SDK 8.0+
- RabbitMQ 3.8+ (quorum queues)

## Install

If package is available on NuGet:

```bash
dotnet add package Rmq.CloudEvents
```

In this repository (local development), use project reference:

```xml
<ProjectReference Include="../../src/Rmq.CloudEvents/Rmq.CloudEvents.csproj" />
```

## Quick Start

### 1) Configure services

```csharp
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Extensions;

builder.Services.AddRmqCloudEvents(options =>
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

### 2) Register a consumer handler

```csharp
using Rmq.CloudEvents.Consuming;

builder.Services.AddRmqConsumer<OrderCreated, OrderCreatedHandler>("orders");

public sealed class OrderCreatedHandler : IRmqMessageHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated message, MessageContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Order {message.OrderId} received from {context.QueueName}, eventId={context.EventId}");
        return Task.CompletedTask;
    }
}

public sealed record OrderCreated(int OrderId, string CustomerId, decimal Total);
```

Register a topic consumer (pub/sub):

```csharp
builder.Services.AddRmqTopicConsumer<OrderEvent, OrderAuditHandler>(opts =>
{
    opts.ExchangeName = "business-events";
    opts.QueueName = "order-audit";
    opts.BindingPatterns = ["orders.*"];
});

public sealed record OrderEvent(int OrderId, string CustomerId, string Action);
```

### 3) Publish messages

```csharp
using Rmq.CloudEvents.Publishing;

var publisher = serviceProvider.GetRequiredService<IRmqPublisher>();

await publisher.PublishAsync(
    queueName: "orders",
    payload: new OrderCreated(1, "cust-001", 99.90m),
    cloudEventType: "com.mycompany.order.created.v1",
    cancellationToken: cancellationToken);
```

With custom headers:

```csharp
await publisher.PublishAsync(
    queueName: "orders",
    payload: new OrderCreated(2, "cust-002", 149.50m),
    headers: new Dictionary<string, object>
    {
        ["x-correlation-id"] = "corr-123",
        ["x-tenant"] = "tenant-a"
    },
    cancellationToken: cancellationToken);
```

Publish to topic exchange:

```csharp
await publisher.PublishToTopicAsync(
    exchangeName: "business-events",
    routingKey: "orders.created",
    payload: new OrderEvent(1, "cust-001", "created"),
    cloudEventType: "com.mycompany.order.event.v1",
    cancellationToken: cancellationToken);
```

## Configuration Model

Main root object: `RmqOptions`

- `Connection` (`RmqConnectionOptions`)
  - `HostName`, `Port`, `UserName`, `Password`, `VirtualHost`, `Ssl`, `NetworkRecoveryInterval`
- `DefaultCloudEvents` (`CloudEventsOptions`)
  - `Source`, `DefaultType`, `SpecVersion`
- `DefaultRetry` (`RetryOptions`)
  - `MaxAttempts` (default `5`)
  - `InitialDelay` (default `1s`)
  - `BackoffType` (`Exponential`, `Linear`, `Constant`)
  - `UseJitter` (default `true`)
- `Queues` (`Dictionary<string, QueueOptions>`)
  - Per-queue overrides for quorum size, delivery limit, retry, and DLQ suffix.
- `Exchanges` (`Dictionary<string, ExchangeOptions>`)
  - Named topic/direct exchange declarations used by `PublishToTopicAsync` and topic consumers.

Topic consumer config object: `TopicSubscriptionOptions`

- `ExchangeName` (required)
- `QueueName` (required)
- `BindingPatterns` (required, one or more; supports `*` and `#`)
- `Retry` (optional override)
- `Dlq` (optional override)

## Runtime Behavior

- Publish:
  - Payload is wrapped as CloudEvent JSON (`application/cloudevents+json`).
  - Queue topology is declared (idempotent) before first publish and re-declared after broker-side failures.
  - For topic publish, exchange topology is declared (idempotent) before first publish and re-declared after broker-side failures.
  - Retry policy handles transient RabbitMQ/network errors.
  - Each publish attempt uses a dedicated confirm-enabled channel, avoiding a shared-channel throughput bottleneck.
- Consume:
  - Message is unwrapped from CloudEvent and your handler receives only the payload.
  - On success: ACK.
  - On final failure: NACK with `requeue: false`, message is routed to DLQ.
  - On cancellation during processing: NACK with `requeue: true`.
  - For topic consumers, `MessageContext` also includes `ExchangeName` and `RoutingKey`.
  - If the broker closes the channel, the hosted consumer recreates topology and resumes automatically.

## Observability

The library exposes public identifiers through `Rmq.CloudEvents.Diagnostics.RmqCloudEventsTelemetry`:

- `ActivitySourceName`
- `MeterName`
- `Version`

Typical OpenTelemetry registration:

```csharp
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Rmq.CloudEvents.Diagnostics;

builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddSource(RmqCloudEventsTelemetry.ActivitySourceName))
    .WithMetrics(metrics => metrics
        .AddMeter(RmqCloudEventsTelemetry.MeterName));
```

Metrics currently emitted include:

- `rmq.connection.attempts`, `rmq.connection.successes`, `rmq.connection.failures`, `rmq.connection.duration.ms`
- `rmq.publish.attempts`, `rmq.publish.successes`, `rmq.publish.failures`, `rmq.publish.retries`, `rmq.publish.duration.ms`
- `rmq.consume.attempts`, `rmq.consume.successes`, `rmq.consume.failures`, `rmq.consume.retries`, `rmq.consume.duration.ms`

## Retry and DLX Flow

The flow below summarizes how publish retry, consumer retry, and DLX/DLQ routing behave together.

```mermaid
flowchart TD
    P[Publish request] --> PR{Publish OK?}
    PR -->|yes| Q[Main queue]
    PR -->|no| PE[Publish error]
    Q --> C[Consume message]
    C --> H{Handler OK?}
    H -->|yes| A[ACK]
    H -->|no| R{Retry left?}
    R -->|yes| RH[Retry handler]
    RH --> H
    R -->|no| N[NACK no requeue]
    N --> X[DLX exchange]
    X --> D[Queue.dlq]
```

## Testing

- Unit tests:

```bash
dotnet test tests/Rmq.CloudEvents.Tests
```

- Integration tests (requires Docker):

```bash
dotnet test tests/Rmq.CloudEvents.IntegrationTests
```

Integration coverage includes broker restart and consumer automatic recovery scenarios.

## Performance

The repository now includes two complementary performance layers:

- microbenchmarks with BenchmarkDotNet in `benchmarks/Rmq.CloudEvents.Benchmarks`;
- RabbitMQ-backed throughput, latency, and memory scenarios in `tests/Rmq.CloudEvents.PerformanceTests`.

The dedicated GitHub Actions workflow is `.github/workflows/performance.yml`. It publishes BenchmarkDotNet artifacts plus per-scenario JSON and Markdown summaries for the current operational baseline.

Current baseline captured from the new performance scenarios:

| Scenario | Throughput (msg/s) | Avg Latency (ms) | P95 Latency (ms) | Peak Working Set (MB) |
| --- | ---: | ---: | ---: | ---: |
| `direct-publish-throughput` | 51.39 | 0.00 | 0.00 | 138.23 |
| `direct-publish-throughput-concurrent` | 197.47 | 0.00 | 0.00 | 131.52 |
| `publish-consume-roundtrip` | 53.59 | 17.90 | 30.89 | 140.55 |
| `topic-publish-throughput` | 50.70 | 0.00 | 0.00 | 137.85 |

Methodology and caveats are documented in [docs/performance.md](docs/performance.md).

## CI

GitHub Actions workflow is available at `.github/workflows/ci.yml` and runs:

- restore
- build (`Release`)
- unit tests with coverage
- integration tests
- pack (on `main`)

## Publish to NuGet

NuGet publishing is automated by `.github/workflows/publish-nuget.yml`.

1. Create a repository secret named `NUGET_API_KEY` with a valid nuget.org API key.
2. Release with a tag using semantic version prefixed by `v`:

```bash
git tag v1.0.1
git push origin v1.0.1
```

3. The workflow builds, tests, packs, and pushes `.nupkg`/`.snupkg` to nuget.org.

You can also run it manually from GitHub Actions (`workflow_dispatch`) and provide an optional `version` override.

## Sample App

See `samples/Rmq.CloudEvents.Sample/Program.cs` for an end-to-end example with both direct queue consume/publish and topic exchange pub/sub.

## License

MIT
