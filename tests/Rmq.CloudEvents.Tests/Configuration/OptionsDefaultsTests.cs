using FluentAssertions;
using Rmq.CloudEvents.Configuration;
using Xunit;

namespace Rmq.CloudEvents.Tests.Configuration;

public sealed class OptionsDefaultsTests
{
    [Fact]
    public void RmqConnectionOptions_ShouldHaveExpectedDefaults()
    {
        var options = new RmqConnectionOptions();

        options.HostName.Should().Be("localhost");
        options.Port.Should().Be(5672);
        options.UserName.Should().Be("guest");
        options.Password.Should().Be("guest");
        options.VirtualHost.Should().Be("/");
        options.Ssl.Should().BeNull();
        options.NetworkRecoveryInterval.Should().Be(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void RetryOptions_ShouldHaveExpectedDefaults()
    {
        var options = new RetryOptions();

        options.MaxAttempts.Should().Be(5);
        options.InitialDelay.Should().Be(TimeSpan.FromSeconds(1));
        options.BackoffType.Should().Be(BackoffType.Exponential);
        options.UseJitter.Should().BeTrue();
    }

    [Fact]
    public void DlqOptions_ShouldHaveExpectedDefaults()
    {
        var options = new DlqOptions();

        options.Enabled.Should().BeTrue();
        options.QueueNameSuffix.Should().Be(".dlq");
    }

    [Fact]
    public void QueueOptions_ShouldHaveExpectedDefaults()
    {
        var options = new QueueOptions();

        options.PrefetchCount.Should().Be(0);
        options.QuorumSize.Should().Be(0);
        options.DeliveryLimit.Should().Be(5);
        options.Retry.Should().NotBeNull();
        options.Dlq.Should().NotBeNull();
    }

    [Fact]
    public void CloudEventsOptions_ShouldHaveExpectedDefaults()
    {
        var options = new CloudEventsOptions();

        options.Source.Should().Be(new Uri("/undefined", UriKind.Relative));
        options.DefaultType.Should().Be("com.default.event.v1");
        options.SpecVersion.Should().Be("1.0");
    }

    [Fact]
    public void RmqOptions_ShouldHaveExpectedDefaults()
    {
        var options = new RmqOptions();

        options.Connection.Should().NotBeNull();
        options.DefaultCloudEvents.Should().NotBeNull();
        options.DefaultRetry.Should().NotBeNull();
        options.PublishConfirmTimeout.Should().Be(TimeSpan.FromSeconds(15));
        options.Queues.Should().NotBeNull();
        options.Queues.Should().BeEmpty();
    }
}
