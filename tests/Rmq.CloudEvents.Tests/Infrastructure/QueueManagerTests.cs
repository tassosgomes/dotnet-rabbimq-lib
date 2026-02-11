using FluentAssertions;
using Moq;
using RabbitMQ.Client;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Infrastructure;
using Xunit;

namespace Rmq.CloudEvents.Tests.Infrastructure;

public sealed class QueueManagerTests
{
    [Fact]
    public async Task DeclareQueueWithDlqAsync_ShouldDeclareExpectedTopology()
    {
        var channelMock = new Mock<IChannel>();
        var manager = new QueueManager();

        var options = new QueueOptions
        {
            QuorumSize = 3,
            DeliveryLimit = 7,
            Dlq = new DlqOptions { QueueNameSuffix = ".dlq" }
        };

        await manager.DeclareQueueWithDlqAsync(channelMock.Object, "orders", options);

        var exchangeInvocation = channelMock.Invocations.Single(x => x.Method.Name == nameof(IChannel.ExchangeDeclareAsync));
        exchangeInvocation.Arguments[0].Should().Be("orders.dlx");
        exchangeInvocation.Arguments[1].Should().Be(ExchangeType.Direct);
        exchangeInvocation.Arguments[2].Should().Be(true);
        exchangeInvocation.Arguments[3].Should().Be(false);

        var queueDeclareInvocations = channelMock.Invocations
            .Where(x => x.Method.Name == nameof(IChannel.QueueDeclareAsync))
            .ToList();

        queueDeclareInvocations.Should().HaveCount(2);

        var dlqDeclare = queueDeclareInvocations.Single(x => (string)x.Arguments[0] == "orders.dlq");
        var dlqArgs = dlqDeclare.Arguments[4] as IDictionary<string, object?>;
        dlqArgs.Should().NotBeNull();
        dlqArgs!["x-queue-type"].Should().Be("quorum");

        var mainDeclare = queueDeclareInvocations.Single(x => (string)x.Arguments[0] == "orders");
        var mainArgs = mainDeclare.Arguments[4] as IDictionary<string, object?>;
        mainArgs.Should().NotBeNull();
        mainArgs!["x-queue-type"].Should().Be("quorum");
        mainArgs["x-dead-letter-exchange"].Should().Be("orders.dlx");
        mainArgs["x-dead-letter-routing-key"].Should().Be("orders");
        mainArgs["x-delivery-limit"].Should().Be(7);
        mainArgs["x-quorum-initial-group-size"].Should().Be(3);

        var bindInvocation = channelMock.Invocations.Single(x => x.Method.Name == nameof(IChannel.QueueBindAsync));
        bindInvocation.Arguments[0].Should().Be("orders.dlq");
        bindInvocation.Arguments[1].Should().Be("orders.dlx");
        bindInvocation.Arguments[2].Should().Be("orders");
    }

    [Fact]
    public async Task DeclareQueueWithDlqAsync_ShouldRespectCustomDlqSuffix()
    {
        var channelMock = new Mock<IChannel>();
        var manager = new QueueManager();
        var options = new QueueOptions
        {
            Dlq = new DlqOptions { QueueNameSuffix = ".dead" }
        };

        await manager.DeclareQueueWithDlqAsync(channelMock.Object, "payments", options);

        channelMock.Invocations
            .Where(x => x.Method.Name == nameof(IChannel.QueueDeclareAsync))
            .Any(x => (string)x.Arguments[0] == "payments.dead")
            .Should()
            .BeTrue();
    }

    [Fact]
    public async Task DeclareQueueWithDlqAsync_ShouldSkipQuorumInitialGroupSizeWhenNotConfigured()
    {
        var channelMock = new Mock<IChannel>();
        var manager = new QueueManager();
        var options = new QueueOptions
        {
            QuorumSize = 0,
            DeliveryLimit = 5
        };

        await manager.DeclareQueueWithDlqAsync(channelMock.Object, "billing", options);

        var mainDeclare = channelMock.Invocations
            .Where(x => x.Method.Name == nameof(IChannel.QueueDeclareAsync))
            .Single(x => (string)x.Arguments[0] == "billing");

        var mainArgs = mainDeclare.Arguments[4] as IDictionary<string, object?>;
        mainArgs.Should().NotBeNull();
        mainArgs!.ContainsKey("x-quorum-initial-group-size").Should().BeFalse();
        mainArgs["x-queue-type"].Should().Be("quorum");
        mainArgs["x-dead-letter-exchange"].Should().Be("billing.dlx");
        mainArgs["x-dead-letter-routing-key"].Should().Be("billing");
        mainArgs["x-delivery-limit"].Should().Be(5);
    }
}
