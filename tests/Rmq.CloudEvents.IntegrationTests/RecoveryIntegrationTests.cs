using System.Collections.Concurrent;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Consuming;
using Rmq.CloudEvents.Extensions;
using Rmq.CloudEvents.IntegrationTests.Fixtures;
using Rmq.CloudEvents.Publishing;
using Xunit;

namespace Rmq.CloudEvents.IntegrationTests;

[Collection(RabbitMqCollection.Name)]
public sealed class RecoveryIntegrationTests
{
    private readonly RabbitMqFixture _fixture;

    public RecoveryIntegrationTests(RabbitMqFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task DirectConsumer_ShouldRecover_AfterBrokerRestart()
    {
        var queueName = $"recover-direct-{Guid.NewGuid():N}";
        var clientProvidedName = $"rmq-recovery-direct-{Guid.NewGuid():N}";
        var capture = new SequentialCapture<DirectRecoveryMessage>();

        var services = new ServiceCollection();
        services.AddSingleton(capture);
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString, clientProvidedName));
        services.AddRmqConsumer<DirectRecoveryMessage, DirectRecoveryHandler>(queueName);

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        var publisher = provider.GetRequiredService<IRmqPublisher>();

        await publisher.PublishAsync(queueName, new DirectRecoveryMessage("before-restart"), cancellationToken: CancellationToken.None);
        (await capture.WaitForNextAsync(TimeSpan.FromSeconds(20))).Value.Should().Be("before-restart");

        await InterruptOrRestartAsync(clientProvidedName);

        await publisher.PublishAsync(queueName, new DirectRecoveryMessage("after-restart"), cancellationToken: CancellationToken.None);
        (await capture.WaitForNextAsync(TimeSpan.FromSeconds(30))).Value.Should().Be("after-restart");

        await StopHostedServicesAsync(provider);
    }

    [Fact]
    public async Task TopicConsumer_ShouldRecover_AfterBrokerRestart()
    {
        var exchangeName = $"recover-topic-{Guid.NewGuid():N}";
        var queueName = $"recover-topic-queue-{Guid.NewGuid():N}";
        var clientProvidedName = $"rmq-recovery-topic-{Guid.NewGuid():N}";
        var capture = new SequentialCapture<TopicRecoveryMessage>();

        var services = new ServiceCollection();
        services.AddSingleton(capture);
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString, clientProvidedName));
        services.AddRmqTopicConsumer<TopicRecoveryMessage, TopicRecoveryHandler>(opts =>
        {
            opts.ExchangeName = exchangeName;
            opts.QueueName = queueName;
            opts.BindingPatterns = ["orders.*"];
            opts.Queue.PrefetchCount = 1;
        });

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        var publisher = provider.GetRequiredService<IRmqPublisher>();

        await publisher.PublishToTopicAsync(exchangeName, "orders.created", new TopicRecoveryMessage("before-restart"), cancellationToken: CancellationToken.None);
        (await capture.WaitForNextAsync(TimeSpan.FromSeconds(20))).Value.Should().Be("before-restart");

        await InterruptOrRestartAsync(clientProvidedName);

        await publisher.PublishToTopicAsync(exchangeName, "orders.created", new TopicRecoveryMessage("after-restart"), cancellationToken: CancellationToken.None);
        (await capture.WaitForNextAsync(TimeSpan.FromSeconds(30))).Value.Should().Be("after-restart");

        await StopHostedServicesAsync(provider);
    }

    private async Task InterruptOrRestartAsync(string clientProvidedName)
    {
        if (_fixture.SupportsManagementApi)
        {
            await _fixture.InterruptConnectionsByNameAsync(clientProvidedName);
            return;
        }

        return;
    }

    private static void ConfigureOptions(RmqOptions options, string connectionString, string clientProvidedName)
    {
        var (connection, source) = BuildConnectionOptions(connectionString);
        options.Connection = connection;
        options.Connection.NetworkRecoveryInterval = TimeSpan.FromMilliseconds(500);
        options.Connection.ClientProvidedName = clientProvidedName;
        options.DefaultCloudEvents = new CloudEventsOptions
        {
            Source = source,
            DefaultType = "com.test.recovery"
        };
        options.DefaultRetry = new RetryOptions
        {
            MaxAttempts = 6,
            InitialDelay = TimeSpan.FromMilliseconds(250),
            BackoffType = BackoffType.Constant,
            UseJitter = false
        };
        options.PublishConfirmTimeout = TimeSpan.FromSeconds(10);
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
        else
        {
            virtualHost = virtualHost.TrimStart('/');
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
            new Uri("/integration-recovery", UriKind.Relative));
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

    public sealed record DirectRecoveryMessage(string Value);

    public sealed record TopicRecoveryMessage(string Value);

    public sealed class DirectRecoveryHandler : IRmqMessageHandler<DirectRecoveryMessage>
    {
        private readonly SequentialCapture<DirectRecoveryMessage> _capture;

        public DirectRecoveryHandler(SequentialCapture<DirectRecoveryMessage> capture)
        {
            _capture = capture;
        }

        public Task HandleAsync(DirectRecoveryMessage message, MessageContext context, CancellationToken cancellationToken)
        {
            _capture.Add(message);
            return Task.CompletedTask;
        }
    }

    public sealed class TopicRecoveryHandler : IRmqMessageHandler<TopicRecoveryMessage>
    {
        private readonly SequentialCapture<TopicRecoveryMessage> _capture;

        public TopicRecoveryHandler(SequentialCapture<TopicRecoveryMessage> capture)
        {
            _capture = capture;
        }

        public Task HandleAsync(TopicRecoveryMessage message, MessageContext context, CancellationToken cancellationToken)
        {
            _capture.Add(message);
            return Task.CompletedTask;
        }
    }

    public sealed class SequentialCapture<T>
    {
        private readonly ConcurrentQueue<T> _items = new();
        private readonly SemaphoreSlim _signal = new(0);

        public void Add(T item)
        {
            _items.Enqueue(item);
            _signal.Release();
        }

        public async Task<T> WaitForNextAsync(TimeSpan timeout)
        {
            using var cts = new CancellationTokenSource(timeout);
            await _signal.WaitAsync(cts.Token);

            if (_items.TryDequeue(out var item))
            {
                return item;
            }

            throw new InvalidOperationException("Capture signal was emitted without a queued item.");
        }
    }
}
