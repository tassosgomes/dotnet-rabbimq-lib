using System.Collections.Concurrent;
using System.Diagnostics;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Consuming;
using Rmq.CloudEvents.Extensions;
using Rmq.CloudEvents.Publishing;
using Xunit;

namespace Rmq.CloudEvents.PerformanceTests;

[Collection(PerformanceCollection.Name)]
public sealed class PerformanceScenariosTests
{
    private readonly RabbitMqPerformanceFixture _fixture;

    public PerformanceScenariosTests(RabbitMqPerformanceFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task DirectPublishThroughput_ShouldRecordCurrentBaseline()
    {
        const int messageCount = 1_000;
        const int payloadBytes = 256;

        var queueName = $"perf-direct-{Guid.NewGuid():N}";
        var payload = BenchmarkMessage.Create(payloadBytes, 1);

        var services = new ServiceCollection();
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));

        await using var provider = services.BuildServiceProvider();
        var publisher = provider.GetRequiredService<IRmqPublisher>();

        await using var sampler = new MemorySampler(TimeSpan.FromMilliseconds(25));
        var stopwatch = Stopwatch.StartNew();

        for (var i = 0; i < messageCount; i++)
        {
            await publisher.PublishAsync(queueName, payload, cancellationToken: CancellationToken.None);
        }

        stopwatch.Stop();
        var result = CreateResult(
            scenario: "direct-publish-throughput",
            messageCount: messageCount,
            payloadBytes: payloadBytes,
            parallelism: 1,
            duration: stopwatch.Elapsed,
            latenciesMs: [],
            sampler: sampler);

        result.ThroughputMessagesPerSecond.Should().BeGreaterThan(0);
        PerformanceResultWriter.Write(result);
    }

    [Fact]
    public async Task ConcurrentDirectPublishThroughput_ShouldRecordCurrentBaseline()
    {
        const int messageCount = 1_000;
        const int payloadBytes = 256;
        const int parallelism = 8;

        var queueName = $"perf-direct-concurrent-{Guid.NewGuid():N}";
        var payload = BenchmarkMessage.Create(payloadBytes, 2);

        var services = new ServiceCollection();
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));

        await using var provider = services.BuildServiceProvider();
        var publisher = provider.GetRequiredService<IRmqPublisher>();

        await using var sampler = new MemorySampler(TimeSpan.FromMilliseconds(25));
        var stopwatch = Stopwatch.StartNew();

        await Parallel.ForEachAsync(
            Enumerable.Range(0, messageCount),
            new ParallelOptions
            {
                MaxDegreeOfParallelism = parallelism,
                CancellationToken = CancellationToken.None
            },
            async (_, token) =>
            {
                await publisher.PublishAsync(queueName, payload, cancellationToken: token);
            });

        stopwatch.Stop();
        var result = CreateResult(
            scenario: "direct-publish-throughput-concurrent",
            messageCount: messageCount,
            payloadBytes: payloadBytes,
            parallelism: parallelism,
            duration: stopwatch.Elapsed,
            latenciesMs: [],
            sampler: sampler);

        result.ThroughputMessagesPerSecond.Should().BeGreaterThan(0);
        PerformanceResultWriter.Write(result);
    }

    [Fact]
    public async Task PublishConsumeRoundtrip_ShouldRecordLatencyAndMemory()
    {
        const int messageCount = 200;
        const int payloadBytes = 128;

        var queueName = $"perf-roundtrip-{Guid.NewGuid():N}";
        var capture = new RoundtripCapture(messageCount);

        var services = new ServiceCollection();
        services.AddSingleton(capture);
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));
        services.AddRmqConsumer<RoundtripMessage, RoundtripMessageHandler>(queueName);

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        try
        {
            var publisher = provider.GetRequiredService<IRmqPublisher>();
            await using var sampler = new MemorySampler(TimeSpan.FromMilliseconds(25));
            var stopwatch = Stopwatch.StartNew();

            for (var i = 0; i < messageCount; i++)
            {
                var publishedAt = Stopwatch.GetTimestamp();
                var payload = RoundtripMessage.Create(i, payloadBytes, publishedAt);
                await publisher.PublishAsync(queueName, payload, cancellationToken: CancellationToken.None);
            }

            var latencies = await capture.WaitForAllAsync(TimeSpan.FromSeconds(30));
            stopwatch.Stop();

            var result = CreateResult(
                scenario: "publish-consume-roundtrip",
                messageCount: messageCount,
                payloadBytes: payloadBytes,
                parallelism: 1,
                duration: stopwatch.Elapsed,
                latenciesMs: latencies,
                sampler: sampler);

            result.AverageLatencyMilliseconds.Should().BeGreaterThan(0);
            PerformanceResultWriter.Write(result);
        }
        finally
        {
            await StopHostedServicesAsync(provider);
        }
    }

    [Fact]
    public async Task TopicPublishThroughput_ShouldRecordCurrentBaseline()
    {
        const int messageCount = 500;
        const int payloadBytes = 192;

        var exchangeName = $"perf-topic-{Guid.NewGuid():N}";
        var queueName = $"perf-topic-queue-{Guid.NewGuid():N}";

        var services = new ServiceCollection();
        services.AddRmqCloudEvents(options => ConfigureOptions(options, _fixture.ConnectionString));
        services.AddRmqTopicConsumer<BenchmarkMessage, NoOpTopicHandler>(options =>
        {
            options.ExchangeName = exchangeName;
            options.QueueName = queueName;
            options.BindingPatterns = ["#"];
        });

        await using var provider = services.BuildServiceProvider();
        await StartHostedServicesAsync(provider);

        try
        {
            var publisher = provider.GetRequiredService<IRmqPublisher>();
            var payload = BenchmarkMessage.Create(payloadBytes, 10);

            await using var sampler = new MemorySampler(TimeSpan.FromMilliseconds(25));
            var stopwatch = Stopwatch.StartNew();

            for (var i = 0; i < messageCount; i++)
            {
                await publisher.PublishToTopicAsync(
                    exchangeName,
                    $"bench.{i % 10}",
                    payload,
                    cancellationToken: CancellationToken.None);
            }

            stopwatch.Stop();

            var result = CreateResult(
                scenario: "topic-publish-throughput",
                messageCount: messageCount,
                payloadBytes: payloadBytes,
                parallelism: 1,
                duration: stopwatch.Elapsed,
                latenciesMs: [],
                sampler: sampler);

            result.ThroughputMessagesPerSecond.Should().BeGreaterThan(0);
            PerformanceResultWriter.Write(result);
        }
        finally
        {
            await StopHostedServicesAsync(provider);
        }
    }

    private static PerformanceResult CreateResult(
        string scenario,
        int messageCount,
        int payloadBytes,
        int parallelism,
        TimeSpan duration,
        IReadOnlyList<double> latenciesMs,
        MemorySampler sampler)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var process = Process.GetCurrentProcess();
        process.Refresh();

        return new PerformanceResult(
            Scenario: scenario,
            MessageCount: messageCount,
            PayloadBytes: payloadBytes,
            Parallelism: parallelism,
            DurationMilliseconds: duration.TotalMilliseconds,
            ThroughputMessagesPerSecond: messageCount / duration.TotalSeconds,
            AverageLatencyMilliseconds: latenciesMs.Count == 0 ? 0 : latenciesMs.Average(),
            P95LatencyMilliseconds: latenciesMs.Count == 0 ? 0 : Percentile(latenciesMs, 0.95),
            ManagedMemoryBytes: GC.GetTotalMemory(forceFullCollection: true),
            WorkingSetBytes: process.WorkingSet64,
            PeakWorkingSetBytes: sampler.PeakWorkingSetBytes,
            PrivateMemoryBytes: process.PrivateMemorySize64,
            RecordedAtUtc: DateTimeOffset.UtcNow);
    }

    private static double Percentile(IReadOnlyList<double> values, double percentile)
    {
        var ordered = values.OrderBy(x => x).ToArray();
        var index = (int)Math.Ceiling((ordered.Length - 1) * percentile);
        return ordered[index];
    }

    private static void ConfigureOptions(RmqOptions options, string connectionString)
    {
        var uri = new Uri(connectionString);
        var userInfo = uri.UserInfo.Split(':', 2);
        var username = userInfo.Length > 0 ? Uri.UnescapeDataString(userInfo[0]) : "guest";
        var password = userInfo.Length > 1 ? Uri.UnescapeDataString(userInfo[1]) : "guest";
        var virtualHost = Uri.UnescapeDataString(uri.AbsolutePath);
        if (string.IsNullOrWhiteSpace(virtualHost) || virtualHost == "/")
        {
            virtualHost = "/";
        }

        options.Connection = new RmqConnectionOptions
        {
            HostName = uri.Host,
            Port = uri.Port,
            UserName = username,
            Password = password,
            VirtualHost = virtualHost
        };
        options.DefaultCloudEvents = new CloudEventsOptions
        {
            Source = new Uri("/performance-tests", UriKind.Relative),
            DefaultType = "com.performance.message.v1"
        };
        options.DefaultRetry = new RetryOptions
        {
            MaxAttempts = 2,
            InitialDelay = TimeSpan.FromMilliseconds(20),
            BackoffType = BackoffType.Constant,
            UseJitter = false
        };
        options.PublishConfirmTimeout = TimeSpan.FromSeconds(10);
    }

    private static async Task StartHostedServicesAsync(IServiceProvider provider)
    {
        foreach (var hostedService in provider.GetServices<IHostedService>())
        {
            await hostedService.StartAsync(CancellationToken.None);
        }
    }

    private static async Task StopHostedServicesAsync(IServiceProvider provider)
    {
        foreach (var hostedService in provider.GetServices<IHostedService>())
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    public sealed record BenchmarkMessage(int Sequence, string Data)
    {
        public static BenchmarkMessage Create(int payloadBytes, int sequence)
        {
            return new BenchmarkMessage(sequence, new string('x', payloadBytes));
        }
    }

    public sealed record RoundtripMessage(int Sequence, string Data, long PublishedAtTimestamp)
    {
        public static RoundtripMessage Create(int sequence, int payloadBytes, long publishedAtTimestamp)
        {
            return new RoundtripMessage(sequence, new string('r', payloadBytes), publishedAtTimestamp);
        }
    }

    public sealed class RoundtripCapture
    {
        private readonly int _expectedCount;
        private readonly ConcurrentBag<double> _latencies = [];
        private readonly TaskCompletionSource<IReadOnlyList<double>> _completion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _receivedCount;

        public RoundtripCapture(int expectedCount)
        {
            _expectedCount = expectedCount;
        }

        public void Record(RoundtripMessage message)
        {
            var now = Stopwatch.GetTimestamp();
            var latency = (now - message.PublishedAtTimestamp) * 1000d / Stopwatch.Frequency;
            _latencies.Add(latency);

            if (Interlocked.Increment(ref _receivedCount) == _expectedCount)
            {
                _completion.TrySetResult(_latencies.ToArray());
            }
        }

        public async Task<IReadOnlyList<double>> WaitForAllAsync(TimeSpan timeout)
        {
            using var cts = new CancellationTokenSource(timeout);
            using var _ = cts.Token.Register(() =>
                _completion.TrySetException(new TimeoutException("Timed out waiting for performance roundtrip messages.")));

            return await _completion.Task.ConfigureAwait(false);
        }
    }

    public sealed class RoundtripMessageHandler : IRmqMessageHandler<RoundtripMessage>
    {
        private readonly RoundtripCapture _capture;

        public RoundtripMessageHandler(RoundtripCapture capture)
        {
            _capture = capture;
        }

        public Task HandleAsync(RoundtripMessage message, MessageContext context, CancellationToken cancellationToken)
        {
            _capture.Record(message);
            return Task.CompletedTask;
        }
    }

    public sealed class NoOpTopicHandler : IRmqMessageHandler<BenchmarkMessage>
    {
        public Task HandleAsync(BenchmarkMessage message, MessageContext context, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
