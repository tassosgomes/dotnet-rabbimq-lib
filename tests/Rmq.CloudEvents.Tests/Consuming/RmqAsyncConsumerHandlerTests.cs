using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using Rmq.CloudEvents.CloudEvents;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Consuming;
using Xunit;

namespace Rmq.CloudEvents.Tests.Consuming;

public sealed class RmqAsyncConsumerHandlerTests
{
    [Fact]
    public async Task HandleBasicDeliverAsync_ShouldAck_WhenHandlerSucceeds()
    {
        var channelMock = CreateChannelMock();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        wrapperMock
            .Setup(x => x.Unwrap<TestPayload>(It.IsAny<ReadOnlyMemory<byte>>()))
            .Returns((new TestPayload(1), new CloudEventMetadata("evt-1", new Uri("/svc", UriKind.Relative), "type", DateTimeOffset.UtcNow)));

        var messageHandlerMock = new Mock<IRmqMessageHandler<TestPayload>>();
        var consumer = new RmqAsyncConsumerHandler<TestPayload>(
            channelMock.Object,
            messageHandlerMock.Object,
            wrapperMock.Object,
            new RetryOptions { MaxAttempts = 1, InitialDelay = TimeSpan.Zero, UseJitter = false },
            "orders",
            NullLogger.Instance);

        await consumer.HandleBasicDeliverAsync(
            "tag",
            7,
            false,
            string.Empty,
            "orders",
            new BasicProperties(),
            new ReadOnlyMemory<byte>([1]));

        channelMock.Verify(x => x.BasicAckAsync(7, false, It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(x => x.BasicNackAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task HandleBasicDeliverAsync_ShouldNack_WhenHandlerFailsAfterRetries()
    {
        var channelMock = CreateChannelMock();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        wrapperMock
            .Setup(x => x.Unwrap<TestPayload>(It.IsAny<ReadOnlyMemory<byte>>()))
            .Returns((new TestPayload(1), new CloudEventMetadata("evt-1", new Uri("/svc", UriKind.Relative), "type", DateTimeOffset.UtcNow)));

        var messageHandlerMock = new Mock<IRmqMessageHandler<TestPayload>>();
        messageHandlerMock
            .Setup(x => x.HandleAsync(It.IsAny<TestPayload>(), It.IsAny<MessageContext>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("boom"));

        var consumer = new RmqAsyncConsumerHandler<TestPayload>(
            channelMock.Object,
            messageHandlerMock.Object,
            wrapperMock.Object,
            new RetryOptions { MaxAttempts = 1, InitialDelay = TimeSpan.Zero, UseJitter = false },
            "orders",
            NullLogger.Instance);

        await consumer.HandleBasicDeliverAsync(
            "tag",
            9,
            false,
            string.Empty,
            "orders",
            new BasicProperties(),
            new ReadOnlyMemory<byte>([1]));

        messageHandlerMock.Verify(
            x => x.HandleAsync(It.IsAny<TestPayload>(), It.IsAny<MessageContext>(), It.IsAny<CancellationToken>()),
            Times.Exactly(2));
        channelMock.Verify(x => x.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
        channelMock.Verify(x => x.BasicNackAsync(9, false, false, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task HandleBasicDeliverAsync_ShouldPassUnwrappedPayloadAndContext()
    {
        var channelMock = CreateChannelMock();
        var wrapperMock = new Mock<ICloudEventWrapper>();
        var payload = new TestPayload(123);
        var metadata = new CloudEventMetadata("evt-xyz", new Uri("/my-service", UriKind.Relative), "event.type", DateTimeOffset.UtcNow);

        wrapperMock
            .Setup(x => x.Unwrap<TestPayload>(It.IsAny<ReadOnlyMemory<byte>>()))
            .Returns((payload, metadata));

        MessageContext? captured = null;
        var messageHandlerMock = new Mock<IRmqMessageHandler<TestPayload>>();
        messageHandlerMock
            .Setup(x => x.HandleAsync(payload, It.IsAny<MessageContext>(), It.IsAny<CancellationToken>()))
            .Callback<TestPayload, MessageContext, CancellationToken>((_, ctx, _) => captured = ctx)
            .Returns(Task.CompletedTask);

        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?> { ["x-key"] = "x-value" }
        };

        var consumer = new RmqAsyncConsumerHandler<TestPayload>(
            channelMock.Object,
            messageHandlerMock.Object,
            wrapperMock.Object,
            new RetryOptions { MaxAttempts = 1, InitialDelay = TimeSpan.Zero, UseJitter = false },
            "orders",
            NullLogger.Instance);

        await consumer.HandleBasicDeliverAsync("tag", 11, true, string.Empty, "orders", properties, new ReadOnlyMemory<byte>([1]));

        captured.Should().NotBeNull();
        captured!.EventId.Should().Be("evt-xyz");
        captured.Source.Should().Be(new Uri("/my-service", UriKind.Relative));
        captured.EventType.Should().Be("event.type");
        captured.DeliveryTag.Should().Be(11);
        captured.QueueName.Should().Be("orders");
        captured.AttemptNumber.Should().Be(1);
        captured.Headers.Should().ContainKey("x-key");
    }

    private static Mock<IChannel> CreateChannelMock()
    {
        var channelMock = new Mock<IChannel>();
        channelMock.Setup(x => x.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);
        channelMock.Setup(x => x.BasicNackAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);
        return channelMock;
    }

    public sealed record TestPayload(int Id);
}
