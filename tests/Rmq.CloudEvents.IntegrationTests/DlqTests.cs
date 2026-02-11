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
public sealed class DlqTests
{
    private readonly RabbitMqFixture _fixture;

    public DlqTests(RabbitMqFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task AlwaysFailingHandler_ShouldRouteMessageToDlq_PreservingCloudEvent()
    {
        var queueName = $"orders-dlq-{Guid.NewGuid():N}";
        var dlqName = $"{queueName}.dlq";
        var attemptsCounter = new RetryAttemptCounter();

        var services = new ServiceCollection();
        services.AddSingleton(attemptsCounter);
        services.AddRmqCloudEvents(options =>
        {
            var connection = BuildConnectionOptions(_fixture.ConnectionString);
            options.Connection = connection;
            options.DefaultCloudEvents = new CloudEventsOptions
            {
                Source = new Uri("/integration-tests", UriKind.Relative),
                DefaultType = "com.test.event"
            };
            options.Queues[queueName] = new QueueOptions
            {
                Retry = new RetryOptions
                {
                    MaxAttempts = 3,
                    InitialDelay = TimeSpan.FromMilliseconds(50),
                    BackoffType = BackoffType.Exponential,
                    UseJitter = false
                }
            };
        });
        services.AddRmqConsumer<DlqPayload, AlwaysFailingHandler>(queueName);

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        var publisher = provider.GetRequiredService<IRmqPublisher>();
        await publisher.PublishAsync(queueName, new DlqPayload(9001, "broken-order"), cancellationToken: CancellationToken.None);

        var result = await ReadOneMessageFromQueueAsync(_fixture.ConnectionString, dlqName, TimeSpan.FromSeconds(25), autoAck: true);
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

        orderIdElement.GetInt32().Should().Be(9001);
        attemptsCounter.TotalAttempts.Should().Be(3);

        await StopHostedServicesAsync(provider);
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

    private static RmqConnectionOptions BuildConnectionOptions(string connectionString)
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

        return new RmqConnectionOptions
        {
            HostName = uri.Host,
            Port = uri.Port,
            UserName = username,
            Password = password,
            VirtualHost = virtualHost
        };
    }

    private static async Task<BasicGetResult?> ReadOneMessageFromQueueAsync(string connectionString, string queueName, TimeSpan timeout, bool autoAck)
    {
        var connectionOptions = BuildConnectionOptions(connectionString);
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

    public sealed record DlqPayload(int OrderId, string Name);

    public sealed class RetryAttemptCounter
    {
        private int _totalAttempts;

        public int TotalAttempts => _totalAttempts;

        public void Increment()
        {
            Interlocked.Increment(ref _totalAttempts);
        }
    }

    public sealed class AlwaysFailingHandler : IRmqMessageHandler<DlqPayload>
    {
        private readonly RetryAttemptCounter _attemptCounter;

        public AlwaysFailingHandler(RetryAttemptCounter attemptCounter)
        {
            _attemptCounter = attemptCounter;
        }

        public Task HandleAsync(DlqPayload message, MessageContext context, CancellationToken cancellationToken)
        {
            _attemptCounter.Increment();
            throw new InvalidOperationException("Intentional integration failure");
        }
    }
}
