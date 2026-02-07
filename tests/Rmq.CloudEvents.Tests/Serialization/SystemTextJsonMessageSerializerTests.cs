using System.Text;
using FluentAssertions;
using Rmq.CloudEvents.Exceptions;
using Rmq.CloudEvents.Serialization;
using Xunit;

namespace Rmq.CloudEvents.Tests.Serialization;

public sealed class SystemTextJsonMessageSerializerTests
{
    [Fact]
    public void SerializeDeserialize_ShouldRoundtripSimplePayload()
    {
        var serializer = new SystemTextJsonMessageSerializer();
        var payload = new OrderPayload(123, "cust-001", 99.9m, null);

        var bytes = serializer.Serialize(payload);
        var result = serializer.Deserialize<OrderPayload>(bytes);

        result.Should().BeEquivalentTo(payload);
    }

    [Fact]
    public void Serialize_ShouldUseCamelCaseAndIgnoreNull()
    {
        var serializer = new SystemTextJsonMessageSerializer();
        var payload = new ComplexPayload("Alice", null, [1, 2, 3]);

        var bytes = serializer.Serialize(payload);
        var json = Encoding.UTF8.GetString(bytes);

        json.Should().Contain("\"userName\"");
        json.Should().Contain("\"values\"");
        json.Should().NotContain("optionalNote");
    }

    [Fact]
    public void Deserialize_ShouldHandleComplexNestedPayload()
    {
        var serializer = new SystemTextJsonMessageSerializer();
        var payload = new EnvelopePayload(new OrderPayload(42, "cust-xyz", 12.3m, "note"), ["a", "b"]);

        var bytes = serializer.Serialize(payload);
        var result = serializer.Deserialize<EnvelopePayload>(bytes);

        result.Should().BeEquivalentTo(payload);
    }

    [Fact]
    public void Deserialize_ShouldThrowRmqConsumeException_WhenPayloadIsNullLiteral()
    {
        var serializer = new SystemTextJsonMessageSerializer();
        var bytes = Encoding.UTF8.GetBytes("null");

        var action = () => serializer.Deserialize<OrderPayload>(bytes);

        action.Should().Throw<RmqConsumeException>();
    }

    private sealed record OrderPayload(int OrderId, string CustomerId, decimal Total, string? Note);

    private sealed record ComplexPayload(string UserName, string? OptionalNote, IReadOnlyList<int> Values);

    private sealed record EnvelopePayload(OrderPayload Order, IReadOnlyList<string> Tags);
}
