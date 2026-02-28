using System.Collections.Concurrent;
using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Consuming;
using Rmq.CloudEvents.Exceptions;
using Rmq.CloudEvents.Extensions;
using Rmq.CloudEvents.IntegrationTests.Fixtures;
using Rmq.CloudEvents.Publishing;
using Xunit;

namespace Rmq.CloudEvents.IntegrationTests;

[Collection(RabbitMqCollection.Name)]
public sealed class TopicExchangeTests
{
    private readonly RabbitMqFixture _fixture;

    public TopicExchangeTests(RabbitMqFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task PublishToTopic_ConsumerReceivesMessage()
    {
        var exchangeName = $"test-events-{Guid.NewGuid():N}";
        var queueName = $"test-orders-{Guid.NewGuid():N}";
        var capture = new MessageCapture<TopicOrder>();

        var services = new ServiceCollection();
        services.AddSingleton(capture);
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));
        services.AddRmqTopicConsumer<TopicOrder, CaptureTopicOrderHandler>(opts =>
        {
            opts.ExchangeName = exchangeName;
            opts.QueueName = queueName;
            opts.BindingPatterns = ["orders.*"];
        });

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        var publisher = provider.GetRequiredService<IRmqPublisher>();
        var order = new TopicOrder(1, "customer-1");
        await publisher.PublishToTopicAsync(exchangeName, "orders.created", order);

        var consumed = await capture.WaitAsync(TimeSpan.FromSeconds(20));
        consumed.OrderId.Should().Be(1);
        consumed.CustomerId.Should().Be("customer-1");

        await StopHostedServicesAsync(provider);
    }

    [Fact]
    public async Task TopicConsumer_OnlyReceivesMatchingRoutingKeys()
    {
        var exchangeName = $"selective-{Guid.NewGuid():N}";
        var queueName = $"orders-only-{Guid.NewGuid():N}";
        var capture = new MultiMessageCapture<TopicOrder>();

        var services = new ServiceCollection();
        services.AddSingleton(capture);
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));
        services.AddRmqTopicConsumer<TopicOrder, CaptureMultiTopicOrderHandler>(opts =>
        {
            opts.ExchangeName = exchangeName;
            opts.QueueName = queueName;
            opts.BindingPatterns = ["orders.*"];
        });

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        var publisher = provider.GetRequiredService<IRmqPublisher>();
        await publisher.PublishToTopicAsync(exchangeName, "orders.created", new TopicOrder(1, "c1"));
        var unmatchedPublish = () => publisher.PublishToTopicAsync(exchangeName, "payments.completed", new TopicOrder(2, "c2"));

        await unmatchedPublish.Should().ThrowAsync<RmqPublishException>();

        await Task.Delay(TimeSpan.FromSeconds(3));

        capture.Messages.Should().HaveCount(1);
        capture.Messages.First().OrderId.Should().Be(1);

        await StopHostedServicesAsync(provider);
    }

    [Fact]
    public async Task TopicConsumer_HashBindingReceivesAll()
    {
        var exchangeName = $"hash-{Guid.NewGuid():N}";
        var queueName = $"all-{Guid.NewGuid():N}";
        var capture = new MultiMessageCapture<TopicOrder>();

        var services = new ServiceCollection();
        services.AddSingleton(capture);
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));
        services.AddRmqTopicConsumer<TopicOrder, CaptureMultiTopicOrderHandler>(opts =>
        {
            opts.ExchangeName = exchangeName;
            opts.QueueName = queueName;
            opts.BindingPatterns = ["#"];
        });

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        var publisher = provider.GetRequiredService<IRmqPublisher>();
        await publisher.PublishToTopicAsync(exchangeName, "orders.created", new TopicOrder(1, "c1"));
        await publisher.PublishToTopicAsync(exchangeName, "payments.completed", new TopicOrder(2, "c2"));
        await publisher.PublishToTopicAsync(exchangeName, "users.updated", new TopicOrder(3, "c3"));

        await Task.Delay(TimeSpan.FromSeconds(3));

        capture.Messages.Should().HaveCount(3);

        await StopHostedServicesAsync(provider);
    }

    [Fact]
    public async Task TopicConsumer_MultipleBindingsReceiveFromAll()
    {
        var exchangeName = $"multi-bind-{Guid.NewGuid():N}";
        var queueName = $"multi-{Guid.NewGuid():N}";
        var capture = new MultiMessageCapture<TopicOrder>();

        var services = new ServiceCollection();
        services.AddSingleton(capture);
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));
        services.AddRmqTopicConsumer<TopicOrder, CaptureMultiTopicOrderHandler>(opts =>
        {
            opts.ExchangeName = exchangeName;
            opts.QueueName = queueName;
            opts.BindingPatterns = ["orders.*", "payments.*"];
        });

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        var publisher = provider.GetRequiredService<IRmqPublisher>();
        await publisher.PublishToTopicAsync(exchangeName, "orders.created", new TopicOrder(1, "c1"));
        await publisher.PublishToTopicAsync(exchangeName, "payments.completed", new TopicOrder(2, "c2"));
        var unmatchedPublish = () => publisher.PublishToTopicAsync(exchangeName, "users.deleted", new TopicOrder(3, "c3"));

        await unmatchedPublish.Should().ThrowAsync<RmqPublishException>();

        await Task.Delay(TimeSpan.FromSeconds(3));

        capture.Messages.Should().HaveCount(2);
        capture.Messages.Select(message => message.OrderId).Should().BeEquivalentTo([1, 2]);

        await StopHostedServicesAsync(provider);
    }

    [Fact]
    public async Task Coexistence_DirectPublishAndTopicPublish_WorkSimultaneously()
    {
        var exchangeName = $"coexist-{Guid.NewGuid():N}";
        var topicQueueName = $"topic-q-{Guid.NewGuid():N}";
        var directQueueName = $"direct-q-{Guid.NewGuid():N}";

        var topicCapture = new MessageCapture<TopicOrder>();
        var directCapture = new MessageCapture<DirectMessage>();

        var services = new ServiceCollection();
        services.AddSingleton(topicCapture);
        services.AddSingleton(directCapture);
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));
        services.AddRmqTopicConsumer<TopicOrder, CaptureTopicOrderHandler>(opts =>
        {
            opts.ExchangeName = exchangeName;
            opts.QueueName = topicQueueName;
            opts.BindingPatterns = ["orders.*"];
        });
        services.AddRmqConsumer<DirectMessage, CaptureDirectMessageHandler>(directQueueName);

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        var publisher = provider.GetRequiredService<IRmqPublisher>();
        await publisher.PublishToTopicAsync(exchangeName, "orders.created", new TopicOrder(1, "c1"));
        await publisher.PublishAsync(directQueueName, new DirectMessage("direct-value"));

        var topicResult = await topicCapture.WaitAsync(TimeSpan.FromSeconds(20));
        var directResult = await directCapture.WaitAsync(TimeSpan.FromSeconds(20));

        topicResult.OrderId.Should().Be(1);
        directResult.Value.Should().Be("direct-value");

        await StopHostedServicesAsync(provider);
    }

    [Fact]
    public async Task TopicConsumer_MessageContext_HasExchangeAndRoutingKey()
    {
        var exchangeName = $"ctx-{Guid.NewGuid():N}";
        var queueName = $"ctx-q-{Guid.NewGuid():N}";
        var contextCapture = new MessageCapture<MessageContext>();

        var services = new ServiceCollection();
        services.AddSingleton(contextCapture);
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));
        services.AddRmqTopicConsumer<TopicOrder, CaptureContextHandler>(opts =>
        {
            opts.ExchangeName = exchangeName;
            opts.QueueName = queueName;
            opts.BindingPatterns = ["orders.*"];
        });

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        var publisher = provider.GetRequiredService<IRmqPublisher>();
        await publisher.PublishToTopicAsync(exchangeName, "orders.created", new TopicOrder(1, "c1"));

        var context = await contextCapture.WaitAsync(TimeSpan.FromSeconds(20));
        context.ExchangeName.Should().Be(exchangeName);
        context.RoutingKey.Should().Be("orders.created");

        await StopHostedServicesAsync(provider);
    }

    [Fact]
    public async Task TopicExchange_MultipleConsumersWithDifferentPatterns_ShouldReceiveOnlyMatchingMessages()
    {
        var exchangeName = $"multi-consumer-{Guid.NewGuid():N}";
        var ordersQueueName = $"orders-{Guid.NewGuid():N}";
        var paymentsQueueName = $"payments-{Guid.NewGuid():N}";

        var orderCapture = new MultiMessageCapture<TopicOrder>();
        var paymentCapture = new MultiMessageCapture<TopicPayment>();

        var services = new ServiceCollection();
        services.AddSingleton(orderCapture);
        services.AddSingleton(paymentCapture);
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));
        services.AddRmqTopicConsumer<TopicOrder, CaptureMultiTopicOrderHandler>(opts =>
        {
            opts.ExchangeName = exchangeName;
            opts.QueueName = ordersQueueName;
            opts.BindingPatterns = ["orders.*"];
        });
        services.AddRmqTopicConsumer<TopicPayment, CaptureMultiTopicPaymentHandler>(opts =>
        {
            opts.ExchangeName = exchangeName;
            opts.QueueName = paymentsQueueName;
            opts.BindingPatterns = ["payments.*"];
        });

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        var publisher = provider.GetRequiredService<IRmqPublisher>();
        await publisher.PublishToTopicAsync(exchangeName, "orders.created", new TopicOrder(101, "customer-101"));
        await publisher.PublishToTopicAsync(exchangeName, "payments.completed", new TopicPayment(501, "paid"));
        var unmatchedPublish = () => publisher.PublishToTopicAsync(exchangeName, "users.updated", new TopicOrder(999, "ignored"));

        await unmatchedPublish.Should().ThrowAsync<RmqPublishException>();

        await Task.Delay(TimeSpan.FromSeconds(3));

        orderCapture.Messages.Should().HaveCount(1);
        orderCapture.Messages.Single().OrderId.Should().Be(101);
        paymentCapture.Messages.Should().HaveCount(1);
        paymentCapture.Messages.Single().PaymentId.Should().Be(501);

        await StopHostedServicesAsync(provider);
    }

    [Fact]
    public async Task TopicConsumer_HandlerFailure_ShouldRouteMessageToDlq()
    {
        var exchangeName = $"topic-dlq-{Guid.NewGuid():N}";
        var queueName = $"topic-orders-{Guid.NewGuid():N}";
        var dlqName = $"{queueName}.dlq";
        var attemptsCounter = new RetryAttemptCounter();

        var services = new ServiceCollection();
        services.AddSingleton(attemptsCounter);
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));
        services.AddRmqTopicConsumer<TopicOrder, AlwaysFailingTopicOrderHandler>(opts =>
        {
            opts.ExchangeName = exchangeName;
            opts.QueueName = queueName;
            opts.BindingPatterns = ["orders.*"];
            opts.Queue.Retry = new RetryOptions
            {
                MaxAttempts = 3,
                InitialDelay = TimeSpan.FromMilliseconds(50),
                BackoffType = BackoffType.Exponential,
                UseJitter = false
            };
        });

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        var publisher = provider.GetRequiredService<IRmqPublisher>();
        await publisher.PublishToTopicAsync(exchangeName, "orders.failed", new TopicOrder(9001, "broken-order"));

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

    [Fact]
    public async Task PublishToTopic_ShouldWriteValidCloudEvent_OnWire()
    {
        var exchangeName = $"topic-wire-{Guid.NewGuid():N}";
        var queueName = $"topic-wire-q-{Guid.NewGuid():N}";
        var connectionOptions = BuildConnectionOptions(_fixture.ConnectionString);
        var factory = new ConnectionFactory
        {
            HostName = connectionOptions.HostName,
            Port = connectionOptions.Port,
            UserName = connectionOptions.UserName,
            Password = connectionOptions.Password,
            VirtualHost = connectionOptions.VirtualHost
        };

        await using (var connection = await factory.CreateConnectionAsync())
        await using (var channel = await connection.CreateChannelAsync())
        {
            await channel.ExchangeDeclareAsync(
                exchange: exchangeName,
                type: ExchangeType.Topic,
                durable: true,
                autoDelete: false,
                arguments: null,
                cancellationToken: CancellationToken.None);

            await channel.QueueDeclareAsync(
                queue: queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null,
                cancellationToken: CancellationToken.None);

            await channel.QueueBindAsync(
                queue: queueName,
                exchange: exchangeName,
                routingKey: "orders.created",
                arguments: null,
                cancellationToken: CancellationToken.None);
        }

        var services = new ServiceCollection();
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));

        await using var provider = services.BuildServiceProvider();
        var publisher = provider.GetRequiredService<IRmqPublisher>();
        await publisher.PublishToTopicAsync(exchangeName, "orders.created", new TopicOrder(777, "wire-customer"));

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

    private static void ConfigureOptions(RmqOptions options, string connectionString)
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

        options.Connection = new RmqConnectionOptions
        {
            HostName = uri.Host,
            Port = uri.Port,
            UserName = username,
            Password = password,
            VirtualHost = virtualHost
        };
        options.DefaultCloudEvents = new CloudEventsOptions
        {
            Source = new Uri("/integration-tests", UriKind.Relative),
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

    public sealed record TopicOrder(int OrderId, string CustomerId);

    public sealed record TopicPayment(int PaymentId, string Status);

    public sealed record DirectMessage(string Value);

    public sealed class CaptureTopicOrderHandler : IRmqMessageHandler<TopicOrder>
    {
        private readonly MessageCapture<TopicOrder> _capture;

        public CaptureTopicOrderHandler(MessageCapture<TopicOrder> capture)
        {
            _capture = capture;
        }

        public Task HandleAsync(TopicOrder message, MessageContext context, CancellationToken cancellationToken)
        {
            _capture.Set(message);
            return Task.CompletedTask;
        }
    }

    public sealed class CaptureMultiTopicOrderHandler : IRmqMessageHandler<TopicOrder>
    {
        private readonly MultiMessageCapture<TopicOrder> _capture;

        public CaptureMultiTopicOrderHandler(MultiMessageCapture<TopicOrder> capture)
        {
            _capture = capture;
        }

        public Task HandleAsync(TopicOrder message, MessageContext context, CancellationToken cancellationToken)
        {
            _capture.Add(message);
            return Task.CompletedTask;
        }
    }

    public sealed class CaptureDirectMessageHandler : IRmqMessageHandler<DirectMessage>
    {
        private readonly MessageCapture<DirectMessage> _capture;

        public CaptureDirectMessageHandler(MessageCapture<DirectMessage> capture)
        {
            _capture = capture;
        }

        public Task HandleAsync(DirectMessage message, MessageContext context, CancellationToken cancellationToken)
        {
            _capture.Set(message);
            return Task.CompletedTask;
        }
    }

    public sealed class CaptureMultiTopicPaymentHandler : IRmqMessageHandler<TopicPayment>
    {
        private readonly MultiMessageCapture<TopicPayment> _capture;

        public CaptureMultiTopicPaymentHandler(MultiMessageCapture<TopicPayment> capture)
        {
            _capture = capture;
        }

        public Task HandleAsync(TopicPayment message, MessageContext context, CancellationToken cancellationToken)
        {
            _capture.Add(message);
            return Task.CompletedTask;
        }
    }

    public sealed class CaptureContextHandler : IRmqMessageHandler<TopicOrder>
    {
        private readonly MessageCapture<MessageContext> _capture;

        public CaptureContextHandler(MessageCapture<MessageContext> capture)
        {
            _capture = capture;
        }

        public Task HandleAsync(TopicOrder message, MessageContext context, CancellationToken cancellationToken)
        {
            _capture.Set(context);
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

    public sealed class RetryAttemptCounter
    {
        private int _totalAttempts;

        public int TotalAttempts => _totalAttempts;

        public void Increment()
        {
            Interlocked.Increment(ref _totalAttempts);
        }
    }

    public sealed class AlwaysFailingTopicOrderHandler : IRmqMessageHandler<TopicOrder>
    {
        private readonly RetryAttemptCounter _attemptCounter;

        public AlwaysFailingTopicOrderHandler(RetryAttemptCounter attemptCounter)
        {
            _attemptCounter = attemptCounter;
        }

        public Task HandleAsync(TopicOrder message, MessageContext context, CancellationToken cancellationToken)
        {
            _attemptCounter.Increment();
            throw new InvalidOperationException("Intentional integration failure");
        }
    }

    public sealed class MultiMessageCapture<T>
    {
        private readonly ConcurrentBag<T> _messages = new();

        public IReadOnlyCollection<T> Messages => _messages.ToArray();

        public void Add(T value)
        {
            _messages.Add(value);
        }
    }
}
