using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Rmq.CloudEvents.CloudEvents;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Connection;
using Rmq.CloudEvents.Consuming;
using Rmq.CloudEvents.Infrastructure;
using Xunit;

namespace Rmq.CloudEvents.Tests.Consuming;

public sealed class RmqTopicConsumerTests
{
    [Fact]
    public async Task StartAsync_ShouldCallDeclareExchangeAndBindingsAsync_WithCorrectParameters()
    {
        // Arrange
        var channelMock = new Mock<IChannel>();
        channelMock.SetupGet(x => x.IsOpen).Returns(true);

        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        var connectionMock = new Mock<IConnection>();
        connectionManagerMock.Setup(x => x.GetConnectionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(connectionMock.Object);
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        var handlerMock = new Mock<IRmqMessageHandler<TestPayload>>();

        var subscription = new TopicSubscriptionOptions
        {
            ExchangeName = "business-events",
            QueueName = "order-audit-queue",
            BindingPatterns = new List<string> { "orders.*" }
        };

        await using var consumer = new RmqTopicConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            handlerMock.Object.HandleAsync,
            new RmqOptions(),
            subscription,
            NullLogger<RmqTopicConsumer<TestPayload>>.Instance);

        // Act
        await consumer.StartAsync();

        // Assert
        queueManagerMock.Verify(x => x.DeclareExchangeAndBindingsAsync(
            channelMock.Object,
            "business-events",
            "order-audit-queue",
            It.Is<IReadOnlyList<string>>(patterns => patterns.Count == 1 && patterns[0] == "orders.*"),
            It.IsAny<QueueOptions>(),
            It.IsAny<ExchangeOptions>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StartAsync_ShouldCallBasicConsumeAsync_WithCorrectQueue()
    {
        // Arrange
        var channelMock = new Mock<IChannel>();
        channelMock.SetupGet(x => x.IsOpen).Returns(true);
        channelMock
            .Setup(x => x.BasicConsumeAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<IDictionary<string, object?>?>(),
                It.IsAny<IAsyncBasicConsumer>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync("consumer-tag-123");

        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        var connectionMock = new Mock<IConnection>();
        connectionManagerMock.Setup(x => x.GetConnectionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(connectionMock.Object);
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        var handlerMock = new Mock<IRmqMessageHandler<TestPayload>>();

        var subscription = new TopicSubscriptionOptions
        {
            ExchangeName = "test-exchange",
            QueueName = "test-queue",
            BindingPatterns = new List<string> { "#" }
        };

        await using var consumer = new RmqTopicConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            handlerMock.Object.HandleAsync,
            new RmqOptions(),
            subscription,
            NullLogger<RmqTopicConsumer<TestPayload>>.Instance);

        // Act
        await consumer.StartAsync();

        // Assert
        channelMock.Verify(x => x.BasicConsumeAsync(
            "test-queue",
            false,
            It.IsAny<string>(),
            false,
            false,
            null,
            It.IsAny<IAsyncBasicConsumer>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StartAsync_ShouldBeIdempotent_WhenAlreadyStarted()
    {
        // Arrange
        var channelMock = new Mock<IChannel>();
        channelMock.SetupGet(x => x.IsOpen).Returns(true);
        channelMock
            .Setup(x => x.BasicConsumeAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<IDictionary<string, object?>?>(),
                It.IsAny<IAsyncBasicConsumer>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync("consumer-tag");

        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        var connectionMock = new Mock<IConnection>();
        connectionManagerMock.Setup(x => x.GetConnectionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(connectionMock.Object);
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        var handlerMock = new Mock<IRmqMessageHandler<TestPayload>>();

        var subscription = new TopicSubscriptionOptions
        {
            ExchangeName = "exchange",
            QueueName = "queue",
            BindingPatterns = new List<string> { "test.*" }
        };

        await using var consumer = new RmqTopicConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            handlerMock.Object.HandleAsync,
            new RmqOptions(),
            subscription,
            NullLogger<RmqTopicConsumer<TestPayload>>.Instance);

        // Act
        await consumer.StartAsync();
        await consumer.StartAsync();
        await consumer.StartAsync();

        // Assert
        connectionManagerMock.Verify(x => x.GetConnectionAsync(It.IsAny<CancellationToken>()), Times.Once);
        connectionManagerMock.Verify(x => x.CreateChannelAsync(It.IsAny<CancellationToken>()), Times.Once);
        queueManagerMock.Verify(x => x.DeclareExchangeAndBindingsAsync(
            It.IsAny<IChannel>(),
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<IReadOnlyList<string>>(),
            It.IsAny<QueueOptions>(),
            It.IsAny<ExchangeOptions>(),
            It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(x => x.BasicConsumeAsync(
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<IDictionary<string, object?>?>(),
            It.IsAny<IAsyncBasicConsumer>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StopAsync_ShouldCancelConsumerAndCloseChannel()
    {
        // Arrange
        var channelMock = new Mock<IChannel>();
        channelMock.SetupGet(x => x.IsOpen).Returns(true);
        channelMock
            .Setup(x => x.BasicConsumeAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<IDictionary<string, object?>?>(),
                It.IsAny<IAsyncBasicConsumer>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync("test-consumer-tag");
        channelMock
            .Setup(x => x.BasicCancelAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        channelMock
            .Setup(x => x.CloseAsync(It.IsAny<ushort>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        var connectionMock = new Mock<IConnection>();
        connectionManagerMock.Setup(x => x.GetConnectionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(connectionMock.Object);
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        var handlerMock = new Mock<IRmqMessageHandler<TestPayload>>();

        var subscription = new TopicSubscriptionOptions
        {
            ExchangeName = "events",
            QueueName = "event-queue",
            BindingPatterns = new List<string> { "event.*" }
        };

        await using var consumer = new RmqTopicConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            handlerMock.Object.HandleAsync,
            new RmqOptions(),
            subscription,
            NullLogger<RmqTopicConsumer<TestPayload>>.Instance);

        // Act
        await consumer.StartAsync();
        await consumer.StopAsync();

        // Assert
        channelMock.Verify(x => x.BasicCancelAsync("test-consumer-tag", false, It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(x => x.CloseAsync(200, "Topic consumer stopped", false, It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(x => x.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task StartAsync_ShouldUseExchangeOptionsFromRmqOptions_WhenAvailable()
    {
        // Arrange
        var channelMock = new Mock<IChannel>();
        channelMock.SetupGet(x => x.IsOpen).Returns(true);

        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        var connectionMock = new Mock<IConnection>();
        connectionManagerMock.Setup(x => x.GetConnectionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(connectionMock.Object);
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        var handlerMock = new Mock<IRmqMessageHandler<TestPayload>>();

        var exchangeOptions = new ExchangeOptions
        {
            Name = "business-events",
            Durable = true,
            AutoDelete = false
        };

        var options = new RmqOptions
        {
            Exchanges = new Dictionary<string, ExchangeOptions>
            {
                ["business-events"] = exchangeOptions
            }
        };

        var subscription = new TopicSubscriptionOptions
        {
            ExchangeName = "business-events",
            QueueName = "orders-queue",
            BindingPatterns = new List<string> { "orders.*" }
        };

        await using var consumer = new RmqTopicConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            handlerMock.Object.HandleAsync,
            options,
            subscription,
            NullLogger<RmqTopicConsumer<TestPayload>>.Instance);

        // Act
        await consumer.StartAsync();

        // Assert
        queueManagerMock.Verify(x => x.DeclareExchangeAndBindingsAsync(
            It.IsAny<IChannel>(),
            "business-events",
            "orders-queue",
            It.IsAny<IReadOnlyList<string>>(),
            It.IsAny<QueueOptions>(),
            exchangeOptions,
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StartAsync_ShouldUseNullExchangeOptions_WhenNotInRmqOptions()
    {
        // Arrange
        var channelMock = new Mock<IChannel>();
        channelMock.SetupGet(x => x.IsOpen).Returns(true);

        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        var connectionMock = new Mock<IConnection>();
        connectionManagerMock.Setup(x => x.GetConnectionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(connectionMock.Object);
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        var handlerMock = new Mock<IRmqMessageHandler<TestPayload>>();

        var subscription = new TopicSubscriptionOptions
        {
            ExchangeName = "events-exchange",
            QueueName = "my-queue",
            BindingPatterns = new List<string> { "event.#" }
        };

        await using var consumer = new RmqTopicConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            handlerMock.Object.HandleAsync,
            new RmqOptions(),
            subscription,
            NullLogger<RmqTopicConsumer<TestPayload>>.Instance);

        // Act
        await consumer.StartAsync();

        // Assert
        queueManagerMock.Verify(x => x.DeclareExchangeAndBindingsAsync(
            It.IsAny<IChannel>(),
            "events-exchange",
            "my-queue",
            It.IsAny<IReadOnlyList<string>>(),
            It.IsAny<QueueOptions>(),
            null,
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public void Constructor_ShouldThrowArgumentNullException_WhenConnectionManagerIsNull()
    {
        // Arrange & Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
        {
            new RmqTopicConsumer<TestPayload>(
                null!,
                new Mock<IQueueManager>().Object,
                new Mock<ICloudEventWrapper>().Object,
                CreateHandlerInvoker(),
                new RmqOptions(),
                new TopicSubscriptionOptions
                {
                    ExchangeName = "exchange",
                    QueueName = "queue",
                    BindingPatterns = new List<string> { "test.*" }
                });
        });

        exception.ParamName.Should().Be("connectionManager");
    }

    [Fact]
    public void Constructor_ShouldThrowArgumentNullException_WhenQueueManagerIsNull()
    {
        // Arrange & Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
        {
            new RmqTopicConsumer<TestPayload>(
                new Mock<IRmqConnectionManager>().Object,
                null!,
                new Mock<ICloudEventWrapper>().Object,
                CreateHandlerInvoker(),
                new RmqOptions(),
                new TopicSubscriptionOptions
                {
                    ExchangeName = "exchange",
                    QueueName = "queue",
                    BindingPatterns = new List<string> { "test.*" }
                });
        });

        exception.ParamName.Should().Be("queueManager");
    }

    [Fact]
    public void Constructor_ShouldThrowArgumentNullException_WhenCloudEventWrapperIsNull()
    {
        // Arrange & Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
        {
            new RmqTopicConsumer<TestPayload>(
                new Mock<IRmqConnectionManager>().Object,
                new Mock<IQueueManager>().Object,
                null!,
                CreateHandlerInvoker(),
                new RmqOptions(),
                new TopicSubscriptionOptions
                {
                    ExchangeName = "exchange",
                    QueueName = "queue",
                    BindingPatterns = new List<string> { "test.*" }
                });
        });

        exception.ParamName.Should().Be("cloudEventWrapper");
    }

    [Fact]
    public void Constructor_ShouldThrowArgumentNullException_WhenMessageHandlerIsNull()
    {
        // Arrange & Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
        {
            new RmqTopicConsumer<TestPayload>(
                new Mock<IRmqConnectionManager>().Object,
                new Mock<IQueueManager>().Object,
                new Mock<ICloudEventWrapper>().Object,
                null!,
                new RmqOptions(),
                new TopicSubscriptionOptions
                {
                    ExchangeName = "exchange",
                    QueueName = "queue",
                    BindingPatterns = new List<string> { "test.*" }
                });
        });

        exception.ParamName.Should().Be("messageHandlerInvoker");
    }

    [Fact]
    public void Constructor_ShouldThrowArgumentNullException_WhenOptionsIsNull()
    {
        // Arrange & Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
        {
            new RmqTopicConsumer<TestPayload>(
                new Mock<IRmqConnectionManager>().Object,
                new Mock<IQueueManager>().Object,
                new Mock<ICloudEventWrapper>().Object,
                CreateHandlerInvoker(),
                null!,
                new TopicSubscriptionOptions
                {
                    ExchangeName = "exchange",
                    QueueName = "queue",
                    BindingPatterns = new List<string> { "test.*" }
                });
        });

        exception.ParamName.Should().Be("options");
    }

    [Fact]
    public void Constructor_ShouldThrowArgumentNullException_WhenSubscriptionIsNull()
    {
        // Arrange & Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
        {
            new RmqTopicConsumer<TestPayload>(
                new Mock<IRmqConnectionManager>().Object,
                new Mock<IQueueManager>().Object,
                new Mock<ICloudEventWrapper>().Object,
                CreateHandlerInvoker(),
                new RmqOptions(),
                null!);
        });

        exception.ParamName.Should().Be("subscription");
    }

    [Fact]
    public async Task StartAsync_ShouldSupportMultipleBindingPatterns()
    {
        // Arrange
        var channelMock = new Mock<IChannel>();
        channelMock.SetupGet(x => x.IsOpen).Returns(true);

        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        var connectionMock = new Mock<IConnection>();
        connectionManagerMock.Setup(x => x.GetConnectionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(connectionMock.Object);
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        var handlerMock = new Mock<IRmqMessageHandler<TestPayload>>();

        var subscription = new TopicSubscriptionOptions
        {
            ExchangeName = "multi-topic",
            QueueName = "multi-queue",
            BindingPatterns = new List<string> { "orders.*", "payments.#", "notifications.*" }
        };

        await using var consumer = new RmqTopicConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            handlerMock.Object.HandleAsync,
            new RmqOptions(),
            subscription,
            NullLogger<RmqTopicConsumer<TestPayload>>.Instance);

        // Act
        await consumer.StartAsync();

        // Assert
        queueManagerMock.Verify(x => x.DeclareExchangeAndBindingsAsync(
            It.IsAny<IChannel>(),
            "multi-topic",
            "multi-queue",
            It.Is<IReadOnlyList<string>>(patterns =>
                patterns.Count == 3 &&
                patterns.Contains("orders.*") &&
                patterns.Contains("payments.#") &&
                patterns.Contains("notifications.*")),
            It.IsAny<QueueOptions>(),
            It.IsAny<ExchangeOptions>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StopAsync_ShouldBeIdempotent_WhenCalledMultipleTimes()
    {
        // Arrange
        var channelMock = new Mock<IChannel>();
        channelMock.SetupGet(x => x.IsOpen).Returns(true);
        channelMock
            .Setup(x => x.BasicConsumeAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<IDictionary<string, object?>?>(),
                It.IsAny<IAsyncBasicConsumer>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync("idem-tag");
        channelMock
            .Setup(x => x.BasicCancelAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        channelMock
            .Setup(x => x.CloseAsync(It.IsAny<ushort>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        var connectionMock = new Mock<IConnection>();
        connectionManagerMock.Setup(x => x.GetConnectionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(connectionMock.Object);
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        var handlerMock = new Mock<IRmqMessageHandler<TestPayload>>();

        var subscription = new TopicSubscriptionOptions
        {
            ExchangeName = "topic-exchange",
            QueueName = "topic-queue",
            BindingPatterns = new List<string> { "topic.*" }
        };

        await using var consumer = new RmqTopicConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            handlerMock.Object.HandleAsync,
            new RmqOptions(),
            subscription,
            NullLogger<RmqTopicConsumer<TestPayload>>.Instance);

        await consumer.StartAsync();

        // Act
        await consumer.StopAsync();
        await consumer.StopAsync();
        await consumer.StopAsync();

        // Assert
        channelMock.Verify(x => x.BasicCancelAsync("idem-tag", false, It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(x => x.CloseAsync(200, "Topic consumer stopped", false, It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(x => x.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task DisposeAsync_ShouldCallStopAsync()
    {
        // Arrange
        var channelMock = new Mock<IChannel>();
        channelMock.SetupGet(x => x.IsOpen).Returns(true);

        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        var connectionMock = new Mock<IConnection>();
        connectionManagerMock.Setup(x => x.GetConnectionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(connectionMock.Object);
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        var handlerMock = new Mock<IRmqMessageHandler<TestPayload>>();

        var subscription = new TopicSubscriptionOptions
        {
            ExchangeName = "dispose-exchange",
            QueueName = "dispose-queue",
            BindingPatterns = new List<string> { "dispose.*" }
        };

        await using var consumer = new RmqTopicConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            handlerMock.Object.HandleAsync,
            new RmqOptions(),
            subscription,
            NullLogger<RmqTopicConsumer<TestPayload>>.Instance);

        await consumer.StartAsync();

        // Act
        await consumer.DisposeAsync();

        // Assert
        channelMock.Verify(x => x.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task TopicConsumer_ShouldRecover_WhenChannelShutdownIsRaised()
    {
        AsyncEventHandler<ShutdownEventArgs>? shutdownHandler = null;

        var firstChannelMock = new Mock<IChannel>();
        firstChannelMock.SetupGet(x => x.IsOpen).Returns(true);
        firstChannelMock
            .SetupAdd(x => x.ChannelShutdownAsync += It.IsAny<AsyncEventHandler<ShutdownEventArgs>>())
            .Callback<AsyncEventHandler<ShutdownEventArgs>>(handler => shutdownHandler += handler);
        firstChannelMock
            .SetupRemove(x => x.ChannelShutdownAsync -= It.IsAny<AsyncEventHandler<ShutdownEventArgs>>())
            .Callback<AsyncEventHandler<ShutdownEventArgs>>(handler => shutdownHandler -= handler);
        firstChannelMock
            .Setup(x => x.BasicConsumeAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<IDictionary<string, object?>?>(),
                It.IsAny<IAsyncBasicConsumer>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync("topic-tag-1");
        firstChannelMock
            .Setup(x => x.BasicCancelAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        firstChannelMock
            .Setup(x => x.CloseAsync(It.IsAny<ushort>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var secondChannelMock = new Mock<IChannel>();
        secondChannelMock.SetupGet(x => x.IsOpen).Returns(true);
        secondChannelMock
            .Setup(x => x.BasicConsumeAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<IDictionary<string, object?>?>(),
                It.IsAny<IAsyncBasicConsumer>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync("topic-tag-2");
        secondChannelMock
            .Setup(x => x.BasicCancelAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        secondChannelMock
            .Setup(x => x.CloseAsync(It.IsAny<ushort>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        var connectionMock = new Mock<IConnection>();
        connectionManagerMock.Setup(x => x.GetConnectionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(connectionMock.Object);
        connectionManagerMock
            .SetupSequence(x => x.CreateChannelAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(firstChannelMock.Object)
            .ReturnsAsync(secondChannelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var subscription = new TopicSubscriptionOptions
        {
            ExchangeName = "recover-exchange",
            QueueName = "recover-queue",
            BindingPatterns = new List<string> { "recover.*" }
        };

        var options = new RmqOptions();
        options.Connection.NetworkRecoveryInterval = TimeSpan.FromMilliseconds(1);

        await using var consumer = new RmqTopicConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            new Mock<ICloudEventWrapper>().Object,
            CreateHandlerInvoker(),
            options,
            subscription,
            NullLogger<RmqTopicConsumer<TestPayload>>.Instance);

        await consumer.StartAsync();
        shutdownHandler.Should().NotBeNull();

        await shutdownHandler!(firstChannelMock.Object, new ShutdownEventArgs(ShutdownInitiator.Library, 500, "test"));
        await Task.Delay(50);

        connectionManagerMock.Verify(x => x.CreateChannelAsync(It.IsAny<CancellationToken>()), Times.Exactly(2));
        queueManagerMock.Verify(
            x => x.DeclareExchangeAndBindingsAsync(It.IsAny<IChannel>(), "recover-exchange", "recover-queue", It.IsAny<IReadOnlyList<string>>(), It.IsAny<QueueOptions>(), It.IsAny<ExchangeOptions>(), It.IsAny<CancellationToken>()),
            Times.Exactly(2));
        secondChannelMock.Verify(
            x => x.BasicConsumeAsync("recover-queue", false, It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object?>?>(), It.IsAny<IAsyncBasicConsumer>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task StartAsync_ShouldThrowInvalidOperationException_WhenQueueNameIsNull()
    {
        // Arrange
        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        var connectionMock = new Mock<IConnection>();
        connectionManagerMock.Setup(x => x.GetConnectionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(connectionMock.Object);
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new Mock<IChannel>().Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        var handlerMock = new Mock<IRmqMessageHandler<TestPayload>>();

        var subscription = new TopicSubscriptionOptions
        {
            ExchangeName = "exchange",
            QueueName = null,
            BindingPatterns = new List<string> { "test.*" }
        };

        await using var consumer = new RmqTopicConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            handlerMock.Object.HandleAsync,
            new RmqOptions(),
            subscription,
            NullLogger<RmqTopicConsumer<TestPayload>>.Instance);

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => consumer.StartAsync());
        exception.Message.Should().Be("QueueName is required for durable topic consumers.");
    }

    public sealed record TestPayload(int Id);

    private static Func<TestPayload, MessageContext, CancellationToken, Task> CreateHandlerInvoker()
    {
        return (_, _, _) => Task.CompletedTask;
    }
}
