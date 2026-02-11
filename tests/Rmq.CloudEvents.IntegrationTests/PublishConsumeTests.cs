using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Consuming;
using Rmq.CloudEvents.Extensions;
using Rmq.CloudEvents.IntegrationTests.Fixtures;
using Rmq.CloudEvents.Publishing;
using Xunit;

namespace Rmq.CloudEvents.IntegrationTests;

[Collection(RabbitMqCollection.Name)]
public sealed class PublishConsumeTests
{
    private readonly RabbitMqFixture _fixture;

    public PublishConsumeTests(RabbitMqFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task PublishConsume_Roundtrip_ShouldPreserveComplexPayload()
    {
        var queueName = $"orders-roundtrip-{Guid.NewGuid():N}";
        var capture = new MessageCapture<OrderCreated>();

        var services = new ServiceCollection();
        services.AddSingleton(capture);
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));
        services.AddRmqConsumer<OrderCreated, CaptureOrderCreatedHandler>(queueName);

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        var publisher = provider.GetRequiredService<IRmqPublisher>();
        var message = new OrderCreated(123, "cust-001", new Address("Main Street", 42), ["vip", "expedite"]);

        await publisher.PublishAsync(queueName, message, cancellationToken: CancellationToken.None);

        var consumed = await capture.WaitAsync(TimeSpan.FromSeconds(20));
        consumed.Should().BeEquivalentTo(message);

        await StopHostedServicesAsync(provider);
    }

    [Fact]
    public async Task Publish_ShouldWriteValidCloudEvent_OnWire()
    {
        var queueName = $"orders-wire-{Guid.NewGuid():N}";

        var services = new ServiceCollection();
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));

        await using var provider = services.BuildServiceProvider();
        var publisher = provider.GetRequiredService<IRmqPublisher>();
        await publisher.PublishAsync(queueName, new WireOrderCreated(777, "wire-customer"), cancellationToken: CancellationToken.None);

        var result = await ReadOneMessageFromQueueAsync(_fixture.ConnectionString, queueName, TimeSpan.FromSeconds(20), autoAck: true);
        result.Should().NotBeNull();

        using var document = JsonDocument.Parse(result!.Body.ToArray());
        var root = document.RootElement;

        root.GetProperty("specversion").GetString().Should().Be("1.0");
        root.GetProperty("id").GetString().Should().NotBeNullOrWhiteSpace();
        root.GetProperty("source").GetString().Should().Be("/integration-tests");
        root.GetProperty("type").GetString().Should().Be("com.test.event");
        DateTimeOffset.TryParse(root.GetProperty("time").GetString(), out _).Should().BeTrue();
        var data = root.GetProperty("data");
        if (!data.TryGetProperty("orderId", out var orderIdElement))
        {
            data.TryGetProperty("OrderId", out orderIdElement).Should().BeTrue();
        }

        if (!data.TryGetProperty("customerId", out var customerIdElement))
        {
            data.TryGetProperty("CustomerId", out customerIdElement).Should().BeTrue();
        }

        orderIdElement.GetInt32().Should().Be(777);
        customerIdElement.GetString().Should().Be("wire-customer");
    }

    [Fact]
    public async Task MultiQueue_ShouldConsumeIndependently()
    {
        var queueA = $"queue-a-{Guid.NewGuid():N}";
        var queueB = $"queue-b-{Guid.NewGuid():N}";

        var captureA = new MessageCapture<QueueAMessage>();
        var captureB = new MessageCapture<QueueBMessage>();

        var services = new ServiceCollection();
        services.AddSingleton(captureA);
        services.AddSingleton(captureB);
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));
        services.AddRmqConsumer<QueueAMessage, QueueAHandler>(queueA);
        services.AddRmqConsumer<QueueBMessage, QueueBHandler>(queueB);

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        var publisher = provider.GetRequiredService<IRmqPublisher>();
        await publisher.PublishAsync(queueA, new QueueAMessage("alpha"), cancellationToken: CancellationToken.None);
        await publisher.PublishAsync(queueB, new QueueBMessage("bravo"), cancellationToken: CancellationToken.None);

        (await captureA.WaitAsync(TimeSpan.FromSeconds(20))).Value.Should().Be("alpha");
        (await captureB.WaitAsync(TimeSpan.FromSeconds(20))).Value.Should().Be("bravo");

        await StopHostedServicesAsync(provider);
    }

    private static void ConfigureOptions(RmqOptions options, string connectionString)
    {
        var (connection, source) = BuildConnectionOptions(connectionString);
        options.Connection = connection;
        options.DefaultCloudEvents = new CloudEventsOptions
        {
            Source = source,
            DefaultType = "com.test.event"
        };
        options.DefaultRetry = new RetryOptions
        {
            MaxAttempts = 2,
            InitialDelay = TimeSpan.FromMilliseconds(50),
            BackoffType = BackoffType.Exponential,
            UseJitter = false
        };
    }

    private static async Task<BasicGetResult?> ReadOneMessageFromQueueAsync(string connectionString, string queueName, TimeSpan timeout, bool autoAck)
    {
        var (connectionOptions, _) = BuildConnectionOptions(connectionString);
        var factory = new ConnectionFactory
        {
            HostName = connectionOptions.HostName,
            Port = connectionOptions.Port,
            UserName = connectionOptions.UserName,
            Password = connectionOptions.Password,
            VirtualHost = connectionOptions.VirtualHost
        };

        await using var connection = await factory.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();

        var deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            var result = await channel.BasicGetAsync(queueName, autoAck);
            if (result is not null)
            {
                return result;
            }

            await Task.Delay(100);
        }

        return null;
    }

    private static async Task StartHostedServicesAsync(IServiceProvider provider)
    {
        foreach (var hostedService in provider.GetServices<IHostedService>())
        {
            await hostedService.StartAsync(CancellationToken.None);
        }
    }

    private static async Task StopHostedServicesAsync(IServiceProvider provider)
    {
        foreach (var hostedService in provider.GetServices<IHostedService>())
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    private static (RmqConnectionOptions Connection, Uri Source) BuildConnectionOptions(string connectionString)
    {
        var uri = new Uri(connectionString);
        var userInfo = uri.UserInfo.Split(':', 2);
        var username = userInfo.Length > 0 ? Uri.UnescapeDataString(userInfo[0]) : "guest";
        var password = userInfo.Length > 1 ? Uri.UnescapeDataString(userInfo[1]) : "guest";

        var virtualHost = Uri.UnescapeDataString(uri.AbsolutePath);
        if (string.IsNullOrWhiteSpace(virtualHost) || virtualHost == "/")
        {
            virtualHost = "/";
        }

        return (
            new RmqConnectionOptions
            {
                HostName = uri.Host,
                Port = uri.Port,
                UserName = username,
                Password = password,
                VirtualHost = virtualHost
            },
            new Uri("/integration-tests", UriKind.Relative));
    }

    public sealed record OrderCreated(int OrderId, string CustomerId, Address ShippingAddress, IReadOnlyList<string> Tags);

    public sealed record Address(string Street, int Number);

    public sealed record WireOrderCreated(int OrderId, string CustomerId);

    public sealed record QueueAMessage(string Value);

    public sealed record QueueBMessage(string Value);

    public sealed class CaptureOrderCreatedHandler : IRmqMessageHandler<OrderCreated>
    {
        private readonly MessageCapture<OrderCreated> _capture;

        public CaptureOrderCreatedHandler(MessageCapture<OrderCreated> capture)
        {
            _capture = capture;
        }

        public Task HandleAsync(OrderCreated message, MessageContext context, CancellationToken cancellationToken)
        {
            _capture.Set(message);
            return Task.CompletedTask;
        }
    }

    public sealed class QueueAHandler : IRmqMessageHandler<QueueAMessage>
    {
        private readonly MessageCapture<QueueAMessage> _capture;

        public QueueAHandler(MessageCapture<QueueAMessage> capture)
        {
            _capture = capture;
        }

        public Task HandleAsync(QueueAMessage message, MessageContext context, CancellationToken cancellationToken)
        {
            _capture.Set(message);
            return Task.CompletedTask;
        }
    }

    public sealed class QueueBHandler : IRmqMessageHandler<QueueBMessage>
    {
        private readonly MessageCapture<QueueBMessage> _capture;

        public QueueBHandler(MessageCapture<QueueBMessage> capture)
        {
            _capture = capture;
        }

        public Task HandleAsync(QueueBMessage message, MessageContext context, CancellationToken cancellationToken)
        {
            _capture.Set(message);
            return Task.CompletedTask;
        }
    }

    public sealed class MessageCapture<T>
    {
        private readonly TaskCompletionSource<T> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Set(T value)
        {
            _tcs.TrySetResult(value);
        }

        public async Task<T> WaitAsync(TimeSpan timeout)
        {
            using var cts = new CancellationTokenSource(timeout);
            var completedTask = await Task.WhenAny(_tcs.Task, Task.Delay(Timeout.InfiniteTimeSpan, cts.Token));
            if (completedTask != _tcs.Task)
            {
                throw new TimeoutException($"Message not received within {timeout}.");
            }

            return await _tcs.Task;
        }
    }
}
