using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Consuming;
using Rmq.CloudEvents.Extensions;
using Xunit;

namespace Rmq.CloudEvents.Tests.Extensions;

public sealed class ServiceCollectionExtensionsTopicTests
{
    [Fact]
    public void AddRmqTopicConsumer_ShouldRegisterHandlerAsTransient()
    {
        var services = new ServiceCollection();
        services.AddRmqCloudEvents(_ => { });

        services.AddRmqTopicConsumer<TestMessage, TestMessageHandler>(options =>
        {
            options.ExchangeName = "business-events";
            options.QueueName = "orders-audit";
            options.BindingPatterns = ["orders.*"];
        });

        services.Should().ContainSingle(descriptor =>
            descriptor.ServiceType == typeof(IRmqMessageHandler<TestMessage>)
            && descriptor.ImplementationType == typeof(TestMessageHandler)
            && descriptor.Lifetime == ServiceLifetime.Transient);
    }

    [Fact]
    public async Task AddRmqTopicConsumer_ShouldRegisterHostedService()
    {
        var services = new ServiceCollection();
        services.AddRmqCloudEvents(_ => { });

        services.AddRmqTopicConsumer<TestMessage, TestMessageHandler>(options =>
        {
            options.ExchangeName = "business-events";
            options.QueueName = "orders-audit";
            options.BindingPatterns = ["orders.*"];
        });

        await using var provider = services.BuildServiceProvider();
        var hostedServices = provider.GetServices<IHostedService>().ToList();

        hostedServices.Should().ContainSingle(service => service is RmqTopicConsumer<TestMessage>);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AddRmqTopicConsumer_ShouldThrowArgumentException_WhenExchangeNameIsInvalid(string? exchangeName)
    {
        var services = new ServiceCollection();
        services.AddRmqCloudEvents(_ => { });

        var action = () => services.AddRmqTopicConsumer<TestMessage, TestMessageHandler>(options =>
        {
            options.ExchangeName = exchangeName!;
            options.QueueName = "orders-audit";
            options.BindingPatterns = ["orders.*"];
        });

        action.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void AddRmqTopicConsumer_ShouldThrowArgumentException_WhenBindingPatternsIsNull()
    {
        var services = new ServiceCollection();
        services.AddRmqCloudEvents(_ => { });

        var action = () => services.AddRmqTopicConsumer<TestMessage, TestMessageHandler>(options =>
        {
            options.ExchangeName = "business-events";
            options.QueueName = "orders-audit";
            options.BindingPatterns = null!;
        });

        action.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void AddRmqTopicConsumer_ShouldThrowArgumentException_WhenBindingPatternsIsEmpty()
    {
        var services = new ServiceCollection();
        services.AddRmqCloudEvents(_ => { });

        var action = () => services.AddRmqTopicConsumer<TestMessage, TestMessageHandler>(options =>
        {
            options.ExchangeName = "business-events";
            options.QueueName = "orders-audit";
            options.BindingPatterns = [];
        });

        action.Should().Throw<ArgumentException>();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AddRmqTopicConsumer_ShouldThrowArgumentException_WhenQueueNameIsInvalid(string? queueName)
    {
        var services = new ServiceCollection();
        services.AddRmqCloudEvents(_ => { });

        var action = () => services.AddRmqTopicConsumer<TestMessage, TestMessageHandler>(options =>
        {
            options.ExchangeName = "business-events";
            options.QueueName = queueName;
            options.BindingPatterns = ["orders.*"];
        });

        action.Should().Throw<ArgumentException>();
    }

    [Fact]
    public async Task AddRmqTopicConsumer_ShouldSupportMultipleDistinctHandlers()
    {
        var services = new ServiceCollection();
        services.AddRmqCloudEvents(_ => { });

        services.AddRmqTopicConsumer<TestMessage, TestMessageHandler>(options =>
        {
            options.ExchangeName = "business-events";
            options.QueueName = "orders-audit";
            options.BindingPatterns = ["orders.*"];
        });

        services.AddRmqTopicConsumer<PaymentMessage, PaymentMessageHandler>(options =>
        {
            options.ExchangeName = "business-events";
            options.QueueName = "payments-processing";
            options.BindingPatterns = ["payments.*"];
        });

        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IRmqMessageHandler<TestMessage>>().Should().BeOfType<TestMessageHandler>();
        provider.GetRequiredService<IRmqMessageHandler<PaymentMessage>>().Should().BeOfType<PaymentMessageHandler>();

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        hostedServices.Should().Contain(service => service is RmqTopicConsumer<TestMessage>);
        hostedServices.Should().Contain(service => service is RmqTopicConsumer<PaymentMessage>);
        hostedServices.Should().HaveCount(2);
    }

    [Fact]
    public async Task AddRmqCloudEventsAndAddRmqConsumer_ShouldContinueWorkingWithTopicRegistrations()
    {
        var services = new ServiceCollection();
        services.AddRmqCloudEvents(_ => { });

        services.AddRmqConsumer<TestMessage, TestMessageHandler>("orders");
        services.AddRmqTopicConsumer<PaymentMessage, PaymentMessageHandler>(options =>
        {
            options.ExchangeName = "business-events";
            options.QueueName = "payments-processing";
            options.BindingPatterns = ["payments.*"];
        });

        await using var provider = services.BuildServiceProvider();

        provider.GetServices<IHostedService>().Should().HaveCount(2);
    }

    public sealed record TestMessage(string Value);

    public sealed record PaymentMessage(string Value);

    public sealed class TestMessageHandler : IRmqMessageHandler<TestMessage>
    {
        public Task HandleAsync(TestMessage message, MessageContext context, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    public sealed class PaymentMessageHandler : IRmqMessageHandler<PaymentMessage>
    {
        public Task HandleAsync(PaymentMessage message, MessageContext context, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
