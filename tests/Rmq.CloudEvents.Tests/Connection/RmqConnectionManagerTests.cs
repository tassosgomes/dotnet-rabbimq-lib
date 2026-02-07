using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Connection;
using Xunit;

namespace Rmq.CloudEvents.Tests.Connection;

public sealed class RmqConnectionManagerTests
{
    [Fact]
    public async Task GetConnectionAsync_ShouldReuseOpenConnection()
    {
        var connectionMock = new Mock<IConnection>();
        connectionMock.SetupGet(x => x.IsOpen).Returns(true);

        var createCount = 0;
        var manager = new RmqConnectionManager(
            new RmqConnectionOptions(),
            _ =>
            {
                createCount++;
                return Task.FromResult(connectionMock.Object);
            },
            NullLogger<RmqConnectionManager>.Instance);

        var first = await manager.GetConnectionAsync();
        var second = await manager.GetConnectionAsync();

        first.Should().BeSameAs(second);
        createCount.Should().Be(1);

        await manager.DisposeAsync();
    }

    [Fact]
    public async Task GetConnectionAsync_ShouldCreateNewConnection_WhenCachedConnectionIsClosed()
    {
        var firstConnectionOpen = true;

        var firstConnectionMock = new Mock<IConnection>();
        firstConnectionMock.SetupGet(x => x.IsOpen).Returns(() => firstConnectionOpen);

        var secondConnectionMock = new Mock<IConnection>();
        secondConnectionMock.SetupGet(x => x.IsOpen).Returns(true);

        var createCount = 0;
        var manager = new RmqConnectionManager(
            new RmqConnectionOptions(),
            _ =>
            {
                createCount++;
                return Task.FromResult(createCount == 1 ? firstConnectionMock.Object : secondConnectionMock.Object);
            },
            NullLogger<RmqConnectionManager>.Instance);

        var first = await manager.GetConnectionAsync();
        firstConnectionOpen = false;
        var second = await manager.GetConnectionAsync();

        first.Should().NotBeSameAs(second);
        second.Should().BeSameAs(secondConnectionMock.Object);
        createCount.Should().Be(2);

        await manager.DisposeAsync();
    }

    [Fact]
    public async Task CreateChannelAsync_ShouldCreateChannelFromCurrentConnection()
    {
        var channelMock = new Mock<IChannel>();
        var connectionMock = new Mock<IConnection>();
        connectionMock.SetupGet(x => x.IsOpen).Returns(true);
        connectionMock
            .Setup(x => x.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(channelMock.Object);

        var manager = new RmqConnectionManager(
            new RmqConnectionOptions(),
            _ => Task.FromResult(connectionMock.Object),
            NullLogger<RmqConnectionManager>.Instance);

        var channel = await manager.CreateChannelAsync();

        channel.Should().BeSameAs(channelMock.Object);
        connectionMock.Verify(
            x => x.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()),
            Times.Once);

        await manager.DisposeAsync();
    }
}
