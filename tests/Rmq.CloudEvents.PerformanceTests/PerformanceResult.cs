namespace Rmq.CloudEvents.PerformanceTests;

public sealed record PerformanceResult(
    string Scenario,
    int MessageCount,
    int PayloadBytes,
    int Parallelism,
    double DurationMilliseconds,
    double ThroughputMessagesPerSecond,
    double AverageLatencyMilliseconds,
    double P95LatencyMilliseconds,
    long ManagedMemoryBytes,
    long WorkingSetBytes,
    long PeakWorkingSetBytes,
    long PrivateMemoryBytes,
    DateTimeOffset RecordedAtUtc);
