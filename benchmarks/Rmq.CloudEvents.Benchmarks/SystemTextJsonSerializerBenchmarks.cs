using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Rmq.CloudEvents.Serialization;

namespace Rmq.CloudEvents.Benchmarks;

[MemoryDiagnoser]
[ShortRunJob]
public class SystemTextJsonSerializerBenchmarks
{
    private readonly SystemTextJsonMessageSerializer _serializer = new();
    private readonly SerializerPayload _payload = new(
        OrderId: 42,
        CustomerId: "cust-serialization",
        Total: 199.95m,
        Attributes: new Dictionary<string, string>
        {
            ["channel"] = "benchmark",
            ["region"] = "us-east"
        });

    private byte[] _serialized = [];

    [GlobalSetup]
    public void Setup()
    {
        _serialized = _serializer.Serialize(_payload);
    }

    [Benchmark]
    public byte[] Serialize()
    {
        return _serializer.Serialize(_payload);
    }

    [Benchmark]
    public SerializerPayload Deserialize()
    {
        return _serializer.Deserialize<SerializerPayload>(_serialized);
    }

    public sealed record SerializerPayload(
        int OrderId,
        string CustomerId,
        decimal Total,
        IReadOnlyDictionary<string, string> Attributes);
}
