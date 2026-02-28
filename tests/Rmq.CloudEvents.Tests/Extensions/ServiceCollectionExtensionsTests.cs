using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
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

        provider.GetRequiredService<IOptions<RmqOptions>>().Value.Connection.HostName.Should().Be("localhost");
        provider.GetRequiredService<RmqOptions>().DefaultCloudEvents.Source.Should().Be(new Uri("/my-service", UriKind.Relative));
        provider.GetRequiredService<IRmqConnectionManager>().Should().NotBeNull();
        provider.GetRequiredService<IQueueManager>().Should().NotBeNull();
        provider.GetRequiredService<ICloudEventWrapper>().Should().NotBeNull();
        provider.GetRequiredService<IMessageSerializer>().Should().NotBeNull();
        provider.GetRequiredService<IRmqPublisher>().Should().NotBeNull();
    }

    [Fact]
    public void AddRmqCloudEvents_ShouldRegisterExpectedLifetimes()
    {
        var services = new ServiceCollection();

        services.AddRmqCloudEvents(_ => { });

        services.Should().ContainSingle(descriptor =>
            descriptor.ServiceType == typeof(IRmqConnectionManager)
            && descriptor.ImplementationType == typeof(RmqConnectionManager)
            && descriptor.Lifetime == ServiceLifetime.Singleton);

        services.Should().ContainSingle(descriptor =>
            descriptor.ServiceType == typeof(IQueueManager)
            && descriptor.ImplementationType == typeof(QueueManager)
            && descriptor.Lifetime == ServiceLifetime.Singleton);

        services.Should().ContainSingle(descriptor =>
            descriptor.ServiceType == typeof(ICloudEventWrapper)
            && descriptor.ImplementationType == typeof(CloudEventWrapper)
            && descriptor.Lifetime == ServiceLifetime.Singleton);

        services.Should().ContainSingle(descriptor =>
            descriptor.ServiceType == typeof(IMessageSerializer)
            && descriptor.ImplementationType == typeof(SystemTextJsonMessageSerializer)
            && descriptor.Lifetime == ServiceLifetime.Singleton);

        services.Should().ContainSingle(descriptor =>
            descriptor.ServiceType == typeof(IRmqPublisher)
            && descriptor.ImplementationType == typeof(RmqPublisher)
            && descriptor.Lifetime == ServiceLifetime.Transient);
    }

    [Fact]
    public async Task AddRmqConsumer_ShouldRegisterHandlerAndHostedService()
    {
        var services = new ServiceCollection();
        services.AddRmqCloudEvents(_ => { });

        services.AddRmqConsumer<TestMessage, TestMessageHandler>("orders");

        await using var provider = services.BuildServiceProvider();

        services.Should().ContainSingle(descriptor =>
            descriptor.ServiceType == typeof(TestMessageHandler)
            && descriptor.Lifetime == ServiceLifetime.Transient);

        services.Should().ContainSingle(descriptor =>
            descriptor.ServiceType == typeof(IRmqMessageHandler<TestMessage>)
            && descriptor.Lifetime == ServiceLifetime.Transient);

        services.Should().ContainSingle(descriptor =>
            descriptor.ServiceType == typeof(IHostedService)
            && descriptor.Lifetime == ServiceLifetime.Singleton);

        provider.GetRequiredService<IRmqMessageHandler<TestMessage>>().Should().NotBeNull();
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
