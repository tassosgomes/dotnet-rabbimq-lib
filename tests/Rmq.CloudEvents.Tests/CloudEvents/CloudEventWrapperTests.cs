using System.Text;
using System.Text.Json;
using FluentAssertions;
using Rmq.CloudEvents.CloudEvents;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Exceptions;
using Xunit;

namespace Rmq.CloudEvents.Tests.CloudEvents;

public sealed class CloudEventWrapperTests
{
    [Fact]
    public void WrapUnwrap_ShouldRoundtripSimplePayload()
    {
        var wrapper = CreateWrapper();
        var payload = new PingPayload("ok");

        var bytes = wrapper.Wrap(payload);
        var (result, metadata) = wrapper.Unwrap<PingPayload>(bytes);

        result.Should().BeEquivalentTo(payload);
        metadata.EventId.Should().NotBeNullOrWhiteSpace();
        metadata.Source.Should().Be(new Uri("/my-service", UriKind.Relative));
    }

    [Fact]
    public void WrapUnwrap_ShouldRoundtripPayloadAndMetadata()
    {
        var wrapper = CreateWrapper();
        var payload = new OrderCreated(123, "cust-1", ["vip", "new"]);

        var bytes = wrapper.Wrap(payload);
        var (result, metadata) = wrapper.Unwrap<OrderCreated>(bytes);

        result.Should().BeEquivalentTo(payload);
        metadata.Source.Should().Be(new Uri("/my-service", UriKind.Relative));
        metadata.EventType.Should().Be("com.default.event.v1");
        metadata.EventId.Should().NotBeNullOrWhiteSpace();
        metadata.Timestamp.Should().BeCloseTo(DateTimeOffset.UtcNow, TimeSpan.FromMinutes(1));
    }

    [Fact]
    public void Wrap_ShouldUseCustomEventType_WhenProvided()
    {
        var wrapper = CreateWrapper();

        var bytes = wrapper.Wrap(new OrderCreated(1, "cust", []), "com.custom.event");
        var (_, metadata) = wrapper.Unwrap<OrderCreated>(bytes);

        metadata.EventType.Should().Be("com.custom.event");
    }

    [Fact]
    public void Wrap_ShouldIncludeRequiredCloudEventFields()
    {
        var wrapper = CreateWrapper();

        var bytes = wrapper.Wrap(new OrderCreated(55, "cust-55", []));
        var json = JsonDocument.Parse(bytes);

        json.RootElement.GetProperty("specversion").GetString().Should().Be("1.0");
        json.RootElement.GetProperty("id").GetString().Should().NotBeNullOrWhiteSpace();
        json.RootElement.GetProperty("source").GetString().Should().Be("/my-service");
        json.RootElement.GetProperty("type").GetString().Should().Be("com.default.event.v1");
        json.RootElement.GetProperty("time").GetString().Should().NotBeNullOrWhiteSpace();
        json.RootElement.GetProperty("datacontenttype").GetString().Should().Be("application/json");
        json.RootElement.TryGetProperty("data", out _).Should().BeTrue();
    }

    [Fact]
    public void Unwrap_ShouldThrow_WhenPayloadIsInvalid()
    {
        var wrapper = CreateWrapper();
        var invalidBytes = Encoding.UTF8.GetBytes("{\"invalid\":true}");

        var action = () => wrapper.Unwrap<OrderCreated>(invalidBytes);

        action.Should().Throw<RmqConsumeException>();
    }

    [Fact]
    public void Unwrap_ShouldThrowRmqConsumeException_WhenCloudEventDataIsNull()
    {
        var wrapper = CreateWrapper();
        var bytes = Encoding.UTF8.GetBytes(
            """
            {
              "specversion": "1.0",
              "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
              "source": "/my-service",
              "type": "com.default.event.v1",
              "time": "2026-02-07T14:30:00Z",
              "datacontenttype": "application/json",
              "data": null
            }
            """);

        var action = () => wrapper.Unwrap<OrderCreated>(bytes);

        action.Should().Throw<RmqConsumeException>();
    }

    [Fact]
    public void Wrap_ShouldThrowNotSupportedException_WhenSpecVersionIsUnsupported()
    {
        var wrapper = new CloudEventWrapper(new CloudEventsOptions
        {
            Source = new Uri("/my-service", UriKind.Relative),
            DefaultType = "com.default.event.v1",
            SpecVersion = "0.3"
        });

        var action = () => wrapper.Wrap(new PingPayload("unsupported"));

        action.Should().Throw<NotSupportedException>();
    }

    private static CloudEventWrapper CreateWrapper()
    {
        var options = new CloudEventsOptions
        {
            Source = new Uri("/my-service", UriKind.Relative),
            DefaultType = "com.default.event.v1",
            SpecVersion = "1.0"
        };

        return new CloudEventWrapper(options);
    }

    private sealed record OrderCreated(int OrderId, string CustomerId, IReadOnlyList<string> Tags);

    private sealed record PingPayload(string Status);

}
