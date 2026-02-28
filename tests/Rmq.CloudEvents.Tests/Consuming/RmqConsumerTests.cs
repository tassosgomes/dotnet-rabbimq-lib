using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using Rmq.CloudEvents.CloudEvents;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Connection;
using Rmq.CloudEvents.Consuming;
using Rmq.CloudEvents.Infrastructure;
using Xunit;

namespace Rmq.CloudEvents.Tests.Consuming;

public sealed class RmqConsumerTests
{
    [Fact]
    public async Task StartAsync_ShouldCreateChannelDeclareQueueAndStartConsume()
    {
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

        await using var consumer = new RmqConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            handlerMock.Object.HandleAsync,
            new RmqOptions(),
            "orders",
            NullLogger<RmqConsumer<TestPayload>>.Instance);

        await consumer.StartAsync();

        connectionManagerMock.Verify(x => x.GetConnectionAsync(It.IsAny<CancellationToken>()), Times.Once);
        connectionManagerMock.Verify(x => x.CreateChannelAsync(It.IsAny<CancellationToken>()), Times.Once);
        queueManagerMock.Verify(x => x.DeclareQueueWithDlqAsync(
            channelMock.Object,
            "orders",
            It.IsAny<QueueOptions>(),
            It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(x => x.BasicConsumeAsync(
            "orders",
            false,
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<IDictionary<string, object?>?>(),
            It.IsAny<IAsyncBasicConsumer>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StopAsync_ShouldCancelConsumerAndDisposeChannel()
    {
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

        await using var consumer = new RmqConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            handlerMock.Object.HandleAsync,
            new RmqOptions(),
            "orders",
            NullLogger<RmqConsumer<TestPayload>>.Instance);

        await consumer.StartAsync();
        await consumer.StopAsync();

        channelMock.Verify(x => x.BasicCancelAsync("consumer-tag", false, It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(x => x.CloseAsync(200, "Consumer stopped", false, It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(x => x.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task StartAsync_ShouldUseQueueSpecificRetryOptions_WhenAvailable()
    {
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

        QueueOptions? declaredOptions = null;
        var queueManagerMock = new Mock<IQueueManager>();
        queueManagerMock
            .Setup(x => x.DeclareQueueWithDlqAsync(
                It.IsAny<IChannel>(),
                It.IsAny<string>(),
                It.IsAny<QueueOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback<IChannel, string, QueueOptions, CancellationToken>((_, _, opts, _) => declaredOptions = opts)
            .Returns(Task.CompletedTask);

        var options = new RmqOptions
        {
            Queues = new Dictionary<string, QueueOptions>
            {
                ["orders"] = new QueueOptions
                {
                    Retry = new RetryOptions { MaxAttempts = 9, InitialDelay = TimeSpan.FromMilliseconds(1) }
                }
            }
        };

        await using var consumer = new RmqConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            new Mock<ICloudEventWrapper>().Object,
            CreateHandlerInvoker(),
            options,
            "orders",
            NullLogger<RmqConsumer<TestPayload>>.Instance);

        await consumer.StartAsync();

        declaredOptions.Should().NotBeNull();
        declaredOptions!.Retry.MaxAttempts.Should().Be(9);
    }

    [Fact]
    public async Task StartAsync_ShouldBeThreadSafeAndIdempotent_WhenCalledConcurrently()
    {
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

        await using var consumer = new RmqConsumer<TestPayload>(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            new Mock<ICloudEventWrapper>().Object,
            CreateHandlerInvoker(),
            new RmqOptions(),
            "orders",
            NullLogger<RmqConsumer<TestPayload>>.Instance);

        await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => consumer.StartAsync()));

        connectionManagerMock.Verify(x => x.GetConnectionAsync(It.IsAny<CancellationToken>()), Times.Once);
        connectionManagerMock.Verify(x => x.CreateChannelAsync(It.IsAny<CancellationToken>()), Times.Once);
        queueManagerMock.Verify(x => x.DeclareQueueWithDlqAsync(
            channelMock.Object,
            "orders",
            It.IsAny<QueueOptions>(),
            It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(x => x.BasicConsumeAsync(
            "orders",
            false,
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<IDictionary<string, object?>?>(),
            It.IsAny<IAsyncBasicConsumer>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StopAsync_ShouldBeThreadSafeAndIdempotent_WhenCalledConcurrently()
    {
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

        await using var consumer = new RmqConsumer<TestPayload>(
            connectionManagerMock.Object,
            new Mock<IQueueManager>().Object,
            new Mock<ICloudEventWrapper>().Object,
            CreateHandlerInvoker(),
            new RmqOptions(),
            "orders",
            NullLogger<RmqConsumer<TestPayload>>.Instance);

        await consumer.StartAsync();
        await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => consumer.StopAsync()));

        channelMock.Verify(x => x.BasicCancelAsync("consumer-tag", false, It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(x => x.CloseAsync(200, "Consumer stopped", false, It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(x => x.DisposeAsync(), Times.Once);
    }

    public sealed record TestPayload(int Id);

    private static Func<TestPayload, MessageContext, CancellationToken, Task> CreateHandlerInvoker()
    {
        return (_, _, _) => Task.CompletedTask;
    }
}
