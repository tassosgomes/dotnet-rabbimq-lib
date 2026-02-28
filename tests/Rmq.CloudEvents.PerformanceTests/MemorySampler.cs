using System.Diagnostics;

namespace Rmq.CloudEvents.PerformanceTests;

internal sealed class MemorySampler : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _samplingTask;

    public MemorySampler(TimeSpan interval)
    {
        _samplingTask = SampleAsync(interval, _cts.Token);
    }

    public long PeakWorkingSetBytes { get; private set; }

    public long PeakPrivateMemoryBytes { get; private set; }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync().ConfigureAwait(false);
        await _samplingTask.ConfigureAwait(false);
        _cts.Dispose();
    }

    private async Task SampleAsync(TimeSpan interval, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var process = Process.GetCurrentProcess();
            process.Refresh();

            PeakWorkingSetBytes = Math.Max(PeakWorkingSetBytes, process.WorkingSet64);
            PeakPrivateMemoryBytes = Math.Max(PeakPrivateMemoryBytes, process.PrivateMemorySize64);

            try
            {
                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}
