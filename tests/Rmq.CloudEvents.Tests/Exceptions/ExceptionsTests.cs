using FluentAssertions;
using Rmq.CloudEvents.Exceptions;
using Xunit;

namespace Rmq.CloudEvents.Tests.Exceptions;

public sealed class ExceptionsTests
{
    [Fact]
    public void RmqCloudEventsException_ShouldStoreMessage()
    {
        var exception = new RmqCloudEventsException("base error");

        exception.Message.Should().Be("base error");
    }

    [Fact]
    public void RmqCloudEventsException_ShouldStoreInnerException()
    {
        var inner = new InvalidOperationException("inner");
        var exception = new RmqCloudEventsException("base error", inner);

        exception.Message.Should().Be("base error");
        exception.InnerException.Should().BeSameAs(inner);
    }

    [Fact]
    public void RmqConnectionException_ShouldInheritBaseException()
    {
        var exception = new RmqConnectionException("connection error");

        exception.Should().BeOfType<RmqConnectionException>();
        exception.Should().BeAssignableTo<RmqCloudEventsException>();
        exception.Message.Should().Be("connection error");
    }

    [Fact]
    public void RmqPublishException_ShouldExposeDiagnosticProperties()
    {
        var inner = new TimeoutException("timeout");
        var exception = new RmqPublishException("orders", 5, inner);

        exception.QueueName.Should().Be("orders");
        exception.AttemptsExhausted.Should().Be(5);
        exception.InnerException.Should().BeSameAs(inner);
        exception.Message.Should().Contain("orders");
        exception.Message.Should().Contain("5");
    }

    [Fact]
    public void RmqConsumeException_ShouldInheritBaseException()
    {
        var exception = new RmqConsumeException("consume error");

        exception.Should().BeOfType<RmqConsumeException>();
        exception.Should().BeAssignableTo<RmqCloudEventsException>();
        exception.Message.Should().Be("consume error");
    }
}
