using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using Rmq.CloudEvents.CloudEvents;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Connection;
using Rmq.CloudEvents.Exceptions;
using Rmq.CloudEvents.Infrastructure;
using Rmq.CloudEvents.Publishing;
using Xunit;

namespace Rmq.CloudEvents.Tests.Publishing;

public sealed class RmqPublisherTests
{
    [Fact]
    public async Task PublishAsync_ShouldPublishCloudEventWithPersistentDelivery()
    {
        var channelMock = CreateChannelMock();
        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        var body = new ReadOnlyMemory<byte>([1, 2, 3]);
        wrapperMock.Setup(x => x.Wrap(It.IsAny<SamplePayload>(), It.IsAny<string?>())).Returns(body);

        var publisher = CreatePublisher(connectionManagerMock, queueManagerMock, wrapperMock);

        await publisher.PublishAsync("orders", new SamplePayload(123));

        queueManagerMock.Verify(x => x.DeclareQueueWithDlqAsync(
            channelMock.Object,
            "orders",
            It.IsAny<QueueOptions>(),
            It.IsAny<CancellationToken>()), Times.Once);

        var publishInvocation = channelMock.Invocations.Single(x => x.Method.Name == nameof(IChannel.BasicPublishAsync));
        publishInvocation.Arguments[0].Should().Be(string.Empty);
        publishInvocation.Arguments[1].Should().Be("orders");
        publishInvocation.Arguments[2].Should().Be(true);

        var properties = publishInvocation.Arguments[3].Should().BeAssignableTo<BasicProperties>().Subject;
        properties.ContentType.Should().Be("application/cloudevents+json");
        properties.DeliveryMode.Should().Be(DeliveryModes.Persistent);

        var publishedBody = publishInvocation.Arguments[4].Should().BeOfType<ReadOnlyMemory<byte>>().Subject;
        publishedBody.ToArray().Should().Equal(body.ToArray());
    }

    [Fact]
    public async Task PublishAsync_ShouldRetryOnTransientFailure()
    {
        var attempt = 0;
        var channelMock = CreateChannelMock(() =>
        {
            attempt++;
            if (attempt == 1)
            {
                throw new TimeoutException("transient");
            }
        });

        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        wrapperMock.Setup(x => x.Wrap(It.IsAny<SamplePayload>(), It.IsAny<string?>())).Returns(new ReadOnlyMemory<byte>([1]));

        var options = CreateOptions();
        options.DefaultRetry.MaxAttempts = 2;
        options.DefaultRetry.InitialDelay = TimeSpan.Zero;

        await using var publisher = new RmqPublisher(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            options,
            NullLogger<RmqPublisher>.Instance);

        await publisher.PublishAsync("orders", new SamplePayload(123));

        channelMock.Invocations.Count(x => x.Method.Name == nameof(IChannel.BasicPublishAsync)).Should().Be(2);
    }

    [Fact]
    public async Task PublishAsync_ShouldThrowRmqPublishException_WhenRetriesExhausted()
    {
        var channelMock = CreateChannelMock(() => throw new IOException("network error"));

        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        wrapperMock.Setup(x => x.Wrap(It.IsAny<SamplePayload>(), It.IsAny<string?>())).Returns(new ReadOnlyMemory<byte>([1]));

        var options = CreateOptions();
        options.DefaultRetry.MaxAttempts = 2;
        options.DefaultRetry.InitialDelay = TimeSpan.Zero;

        await using var publisher = new RmqPublisher(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            options,
            NullLogger<RmqPublisher>.Instance);

        var action = async () => await publisher.PublishAsync("orders", new SamplePayload(999));

        var exception = await action.Should().ThrowAsync<RmqPublishException>();
        exception.Which.QueueName.Should().Be("orders");
        exception.Which.AttemptsExhausted.Should().Be(2);
        channelMock.Invocations.Count(x => x.Method.Name == nameof(IChannel.BasicPublishAsync)).Should().Be(3);
    }

    [Fact]
    public async Task PublishAsync_WithHeaders_ShouldIncludeHeadersInBasicProperties()
    {
        var channelMock = CreateChannelMock();
        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        wrapperMock.Setup(x => x.Wrap(It.IsAny<SamplePayload>(), It.IsAny<string?>())).Returns(new ReadOnlyMemory<byte>([4, 5]));

        var publisher = CreatePublisher(connectionManagerMock, queueManagerMock, wrapperMock);
        var headers = new Dictionary<string, object>
        {
            ["x-correlation-id"] = "corr-1",
            ["x-retry"] = 3
        };

        await publisher.PublishAsync("orders", new SamplePayload(123), headers);

        var publishInvocation = channelMock.Invocations.Single(x => x.Method.Name == nameof(IChannel.BasicPublishAsync));
        var properties = (BasicProperties)publishInvocation.Arguments[3];
        properties.Headers.Should().NotBeNull();
        properties.Headers!["x-correlation-id"].Should().Be("corr-1");
        properties.Headers!["x-retry"].Should().Be(3);
    }

    [Fact]
    public async Task PublishToTopicAsync_ShouldPublishCloudEventWithPersistentDelivery()
    {
        var channelMock = CreateChannelMock();
        channelMock.Setup(x => x.ExchangeDeclareAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<IDictionary<string, object?>>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        var body = new ReadOnlyMemory<byte>([1, 2, 3]);
        wrapperMock.Setup(x => x.Wrap(It.IsAny<SamplePayload>(), It.IsAny<string?>())).Returns(body);

        var publisher = CreatePublisher(connectionManagerMock, queueManagerMock, wrapperMock);

        await publisher.PublishToTopicAsync("orders-exchange", "orders.create", new SamplePayload(123));

        var publishInvocation = channelMock.Invocations.Single(x => x.Method.Name == nameof(IChannel.BasicPublishAsync));
        publishInvocation.Arguments[0].Should().Be("orders-exchange");
        publishInvocation.Arguments[1].Should().Be("orders.create");
        publishInvocation.Arguments[2].Should().Be(false);

        var properties = publishInvocation.Arguments[3].Should().BeAssignableTo<BasicProperties>().Subject;
        properties.ContentType.Should().Be("application/cloudevents+json");
        properties.DeliveryMode.Should().Be(DeliveryModes.Persistent);

        var publishedBody = publishInvocation.Arguments[4].Should().BeOfType<ReadOnlyMemory<byte>>().Subject;
        publishedBody.ToArray().Should().Equal(body.ToArray());
    }

    [Fact]
    public async Task PublishToTopicAsync_ShouldDeclareExchangeAsTopic()
    {
        var channelMock = CreateChannelMock();
        channelMock.Setup(x => x.ExchangeDeclareAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<IDictionary<string, object?>>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

        var connectionManagerMock = new Mock<IRmqConnectionManager>();
        connectionManagerMock.Setup(x => x.CreateChannelAsync(It.IsAny<CancellationToken>())).ReturnsAsync(channelMock.Object);

        var queueManagerMock = new Mock<IQueueManager>();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        wrapperMock.Setup(x => x.Wrap(It.IsAny<SamplePayload>(), It.IsAny<string?>())).Returns(new ReadOnlyMemory<byte>([1]));

        var publisher = CreatePublisher(connectionManagerMock, queueManagerMock, wrapperMock);

        await publisher.PublishToTopicAsync("orders-exchange", "orders.create", new SamplePayload(123));

        channelMock.Verify(x => x.ExchangeDeclareAsync(
            "orders-exchange",
            "topic",
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<IDictionary<string, object?>>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    private static RmqPublisher CreatePublisher(
        Mock<IRmqConnectionManager> connectionManagerMock,
        Mock<IQueueManager> queueManagerMock,
        Mock<ICloudEventWrapper> wrapperMock)
    {
        return new RmqPublisher(
            connectionManagerMock.Object,
            queueManagerMock.Object,
            wrapperMock.Object,
            CreateOptions(),
            NullLogger<RmqPublisher>.Instance);
    }

    private static Mock<IChannel> CreateChannelMock(Action? publishAction = null)
    {
        var channelMock = new Mock<IChannel>();
        channelMock.SetupGet(x => x.IsOpen).Returns(true);
        channelMock
            .Setup(x => x.BasicPublishAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns((string _, string _, bool _, BasicProperties _, ReadOnlyMemory<byte> _, CancellationToken _) =>
            {
                publishAction?.Invoke();
                return ValueTask.CompletedTask;
            });

        return channelMock;
    }

    private static RmqOptions CreateOptions()
    {
        return new RmqOptions
        {
            DefaultRetry = new RetryOptions
            {
                MaxAttempts = 1,
                InitialDelay = TimeSpan.Zero,
                BackoffType = BackoffType.Exponential,
                UseJitter = false
            }
        };
    }

    private sealed record SamplePayload(int OrderId);
}
