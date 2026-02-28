using System.Text;
using System.Text.Json;

namespace Rmq.CloudEvents.PerformanceTests;

internal static class PerformanceResultWriter
{
    private static readonly object SyncRoot = new();

    public static void Write(PerformanceResult result)
    {
        var outputDirectory = ResolveOutputDirectory();
        Directory.CreateDirectory(outputDirectory);

        var scenarioFileName = SanitizeFileName(result.Scenario) + ".json";
        var scenarioPath = Path.Combine(outputDirectory, scenarioFileName);
        File.WriteAllText(
            scenarioPath,
            JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true }),
            Encoding.UTF8);

        var summaryPath = Path.Combine(outputDirectory, "performance-summary.md");

        lock (SyncRoot)
        {
            if (!File.Exists(summaryPath))
            {
                File.WriteAllText(
                    summaryPath,
                    "| Scenario | Messages | Payload (B) | Parallelism | Duration (ms) | Throughput (msg/s) | Avg Latency (ms) | P95 Latency (ms) | Working Set (MB) | Peak Working Set (MB) |\n" +
                    "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
                    Encoding.UTF8);
            }

            var line =
                $"| {result.Scenario} | {result.MessageCount} | {result.PayloadBytes} | {result.Parallelism} | {result.DurationMilliseconds:F2} | {result.ThroughputMessagesPerSecond:F2} | {result.AverageLatencyMilliseconds:F2} | {result.P95LatencyMilliseconds:F2} | {ToMegabytes(result.WorkingSetBytes):F2} | {ToMegabytes(result.PeakWorkingSetBytes):F2} |{Environment.NewLine}";

            File.AppendAllText(summaryPath, line, Encoding.UTF8);
        }
    }

    private static string ResolveOutputDirectory()
    {
        var configured = Environment.GetEnvironmentVariable("PERF_RESULTS_DIR");
        if (!string.IsNullOrWhiteSpace(configured))
        {
            return Path.GetFullPath(configured);
        }

        return Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "artifacts", "performance"));
    }

    private static string SanitizeFileName(string scenario)
    {
        var invalidChars = Path.GetInvalidFileNameChars();
        var sanitizedChars = scenario
            .Select(ch => invalidChars.Contains(ch) ? '-' : ch)
            .ToArray();

        return new string(sanitizedChars).Replace(' ', '-').ToLowerInvariant();
    }

    private static double ToMegabytes(long bytes)
    {
        return bytes / 1024d / 1024d;
    }
}
