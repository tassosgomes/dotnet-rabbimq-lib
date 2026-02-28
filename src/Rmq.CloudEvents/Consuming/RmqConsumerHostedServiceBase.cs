using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Rmq.CloudEvents.CloudEvents;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Connection;

namespace Rmq.CloudEvents.Consuming;

internal abstract class RmqConsumerHostedServiceBase<T> : IHostedService, IRmqConsumer
    where T : class
{
    private readonly IRmqConnectionManager _connectionManager;
    private readonly ICloudEventWrapper _cloudEventWrapper;
    private readonly Func<T, MessageContext, CancellationToken, Task> _messageHandlerInvoker;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);

    private ConsumerSession? _session;
    private CancellationTokenSource? _stoppingCts;
    private Task? _monitorTask;
    private bool _isStarted;
    private bool _disposed;

    protected RmqConsumerHostedServiceBase(
        IRmqConnectionManager connectionManager,
        ICloudEventWrapper cloudEventWrapper,
        Func<T, MessageContext, CancellationToken, Task> messageHandlerInvoker,
        ILogger logger)
    {
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _cloudEventWrapper = cloudEventWrapper ?? throw new ArgumentNullException(nameof(cloudEventWrapper));
        _messageHandlerInvoker = messageHandlerInvoker ?? throw new ArgumentNullException(nameof(messageHandlerInvoker));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected abstract string QueueName { get; }

    protected abstract RetryOptions RetryOptions { get; }

    protected abstract ushort PrefetchCount { get; }

    protected abstract TimeSpan RecoveryDelay { get; }

    protected abstract string StopReplyText { get; }

    protected abstract Task DeclareTopologyAsync(IChannel channel, CancellationToken cancellationToken);

    protected abstract void LogConsumerStarted(string consumerTag);

    protected abstract void LogConsumerStopped();

    protected abstract void LogRecoveryTriggered(object? signal);

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycleLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();

            if (_isStarted)
            {
                return;
            }

            _stoppingCts = new CancellationTokenSource();
            await StartSessionAsync(cancellationToken).ConfigureAwait(false);
            _monitorTask = MonitorAsync(_stoppingCts.Token);
            _isStarted = true;
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        Task? monitorTask;
        ConsumerSession? session;

        await _lifecycleLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!_isStarted && _session is null)
            {
                return;
            }

            _isStarted = false;

            if (_stoppingCts is not null)
            {
                await _stoppingCts.CancelAsync().ConfigureAwait(false);
                _stoppingCts.Dispose();
                _stoppingCts = null;
            }

            monitorTask = _monitorTask;
            _monitorTask = null;
            session = DetachSession();
        }
        finally
        {
            _lifecycleLock.Release();
        }

        await CloseSessionAsync(session, cancellationToken).ConfigureAwait(false);

        if (monitorTask is not null)
        {
            try
            {
                await monitorTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        LogConsumerStopped();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        await StopAsync().ConfigureAwait(false);
        _disposed = true;
        _lifecycleLock.Dispose();
    }

    private async Task StartSessionAsync(CancellationToken cancellationToken)
    {
        await _connectionManager.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        var channel = await _connectionManager.CreateChannelAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            await DeclareTopologyAsync(channel, cancellationToken).ConfigureAwait(false);

            if (PrefetchCount > 0)
            {
                await channel.BasicQosAsync(0, PrefetchCount, false, cancellationToken).ConfigureAwait(false);
            }

            var signal = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);

            AsyncEventHandler<ShutdownEventArgs> onShutdown = (_, args) =>
            {
                signal.TrySetResult(args);
                return Task.CompletedTask;
            };

            AsyncEventHandler<CallbackExceptionEventArgs> onCallbackException = (_, args) =>
            {
                signal.TrySetResult(args.Exception);
                return Task.CompletedTask;
            };

            channel.ChannelShutdownAsync += onShutdown;
            channel.CallbackExceptionAsync += onCallbackException;

            var consumerHandler = new RmqAsyncConsumerHandler<T>(
                channel,
                _messageHandlerInvoker,
                _cloudEventWrapper,
                RetryOptions,
                QueueName,
                _logger);

            var consumerTag = await channel.BasicConsumeAsync(
                queue: QueueName,
                autoAck: false,
                consumerTag: string.Empty,
                noLocal: false,
                exclusive: false,
                arguments: null,
                consumer: consumerHandler,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            _session = new ConsumerSession(channel, consumerTag, signal, onShutdown, onCallbackException);
            LogConsumerStarted(consumerTag);
        }
        catch
        {
            await channel.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task MonitorAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var session = _session;
            if (session is null)
            {
                return;
            }

            object? signal;
            try
            {
                signal = await session.RestartSignal.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            LogRecoveryTriggered(signal);

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(RecoveryDelay, cancellationToken).ConfigureAwait(false);

                    await _lifecycleLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                    try
                    {
                        if (!_isStarted)
                        {
                            return;
                        }

                        var oldSession = DetachSession();
                        await CloseSessionAsync(oldSession, cancellationToken).ConfigureAwait(false);
                        await StartSessionAsync(cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        _lifecycleLock.Release();
                    }

                    break;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Falha ao recriar consumer da queue {QueueName}. Nova tentativa em {RecoveryDelay}.", QueueName, RecoveryDelay);
                }
            }
        }
    }

    private ConsumerSession? DetachSession()
    {
        var session = _session;
        _session = null;
        return session;
    }

    private async Task CloseSessionAsync(ConsumerSession? session, CancellationToken cancellationToken)
    {
        if (session is null)
        {
            return;
        }

        session.Channel.ChannelShutdownAsync -= session.OnShutdown;
        session.Channel.CallbackExceptionAsync -= session.OnCallbackException;

        if (!string.IsNullOrWhiteSpace(session.ConsumerTag) && session.Channel.IsOpen)
        {
            await session.Channel.BasicCancelAsync(session.ConsumerTag, false, cancellationToken).ConfigureAwait(false);
        }

        if (session.Channel.IsOpen)
        {
            await session.Channel.CloseAsync(200, StopReplyText, false, cancellationToken).ConfigureAwait(false);
        }

        await session.Channel.DisposeAsync().ConfigureAwait(false);
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    private sealed record ConsumerSession(
        IChannel Channel,
        string ConsumerTag,
        TaskCompletionSource<object?> RestartSignal,
        AsyncEventHandler<ShutdownEventArgs> OnShutdown,
        AsyncEventHandler<CallbackExceptionEventArgs> OnCallbackException);
}
