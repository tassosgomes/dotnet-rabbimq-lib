using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Rmq.CloudEvents.CloudEvents;
using Rmq.CloudEvents.Configuration;

namespace Rmq.CloudEvents.Benchmarks;

[MemoryDiagnoser]
[ShortRunJob]
public class CloudEventWrapperBenchmarks
{
    private readonly CloudEventWrapper _wrapper = new(new CloudEventsOptions
    {
        Source = new Uri("/benchmarks", UriKind.Relative),
        DefaultType = "com.benchmark.message.v1"
    });

    private readonly BenchmarkPayload _payload = new(
        Id: Guid.NewGuid(),
        CustomerId: "cust-001",
        Amount: 149.90m,
        Tags: ["priority", "benchmark", "direct"]);

    private ReadOnlyMemory<byte> _wrapped;

    [GlobalSetup]
    public void Setup()
    {
        _wrapped = _wrapper.Wrap(_payload);
    }

    [Benchmark]
    public ReadOnlyMemory<byte> Wrap()
    {
        return _wrapper.Wrap(_payload);
    }

    [Benchmark]
    public BenchmarkPayload Unwrap()
    {
        return _wrapper.Unwrap<BenchmarkPayload>(_wrapped).Payload;
    }

    public sealed record BenchmarkPayload(Guid Id, string CustomerId, decimal Amount, IReadOnlyList<string> Tags);
}
