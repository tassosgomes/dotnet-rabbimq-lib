using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rmq.CloudEvents.CloudEvents;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Connection;
using Rmq.CloudEvents.Consuming;
using Rmq.CloudEvents.Extensions;
using Rmq.CloudEvents.Infrastructure;
using Rmq.CloudEvents.Publishing;
using Rmq.CloudEvents.Serialization;
using Xunit;

namespace Rmq.CloudEvents.Tests.Extensions;

public sealed class ServiceCollectionExtensionsTests
{
    [Fact]
    public async Task AddRmqCloudEvents_ShouldRegisterCoreServices()
    {
        var services = new ServiceCollection();

        services.AddRmqCloudEvents(options =>
        {
            options.Connection.HostName = "localhost";
            options.DefaultCloudEvents.Source = new Uri("/my-service", UriKind.Relative);
        });

        await using var provider = services.BuildServiceProvider();

        provider.GetService<RmqOptions>().Should().NotBeNull();
        provider.GetService<IRmqConnectionManager>().Should().NotBeNull();
        provider.GetService<IQueueManager>().Should().NotBeNull();
        provider.GetService<ICloudEventWrapper>().Should().NotBeNull();
        provider.GetService<IMessageSerializer>().Should().NotBeNull();
        provider.GetService<IRmqPublisher>().Should().NotBeNull();
    }

    [Fact]
    public async Task AddRmqConsumer_ShouldRegisterHandlerAndHostedService()
    {
        var services = new ServiceCollection();
        services.AddRmqCloudEvents(_ => { });

        services.AddRmqConsumer<TestMessage, TestMessageHandler>("orders");

        await using var provider = services.BuildServiceProvider();

        provider.GetService<IRmqMessageHandler<TestMessage>>().Should().NotBeNull();
        provider.GetServices<IHostedService>().Should().ContainSingle();
    }

    [Fact]
    public void AddRmqCloudEvents_ShouldThrow_WhenConfigureIsNull()
    {
        var services = new ServiceCollection();

        var action = () => services.AddRmqCloudEvents(null!);

        action.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddRmqConsumer_ShouldThrow_WhenQueueNameIsEmpty()
    {
        var services = new ServiceCollection();
        services.AddRmqCloudEvents(_ => { });

        var action = () => services.AddRmqConsumer<TestMessage, TestMessageHandler>(string.Empty);

        action.Should().Throw<ArgumentException>();
    }

    public sealed record TestMessage(string Value);

    public sealed class TestMessageHandler : IRmqMessageHandler<TestMessage>
    {
        public Task HandleAsync(TestMessage message, MessageContext context, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
