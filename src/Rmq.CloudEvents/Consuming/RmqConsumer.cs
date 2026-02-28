using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Rmq.CloudEvents.CloudEvents;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Connection;
using Rmq.CloudEvents.Infrastructure;

namespace Rmq.CloudEvents.Consuming;

/// <summary>
/// Hosted service para consumo de mensagens RabbitMQ.
/// </summary>
/// <typeparam name="T">Tipo do payload consumido.</typeparam>
internal sealed class RmqConsumer<T> : IHostedService, IRmqConsumer
    where T : class
{
    private readonly IRmqConnectionManager _connectionManager;
    private readonly IQueueManager _queueManager;
    private readonly ICloudEventWrapper _cloudEventWrapper;
    private readonly Func<T, MessageContext, CancellationToken, Task> _messageHandlerInvoker;
    private readonly RmqOptions _options;
    private readonly string _queueName;
    private readonly ILogger<RmqConsumer<T>> _logger;
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);
    private IChannel? _channel;
    private string? _consumerTag;
    private bool _isStarted;

    /// <summary>
    /// Inicializa uma nova instancia de <see cref="RmqConsumer{T}"/>.
    /// </summary>
    public RmqConsumer(
        IRmqConnectionManager connectionManager,
        IQueueManager queueManager,
        ICloudEventWrapper cloudEventWrapper,
        Func<T, MessageContext, CancellationToken, Task> messageHandlerInvoker,
        RmqOptions options,
        string queueName,
        ILogger<RmqConsumer<T>>? logger = null)
    {
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _queueManager = queueManager ?? throw new ArgumentNullException(nameof(queueManager));
        _cloudEventWrapper = cloudEventWrapper ?? throw new ArgumentNullException(nameof(cloudEventWrapper));
        _messageHandlerInvoker = messageHandlerInvoker ?? throw new ArgumentNullException(nameof(messageHandlerInvoker));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _queueName = string.IsNullOrWhiteSpace(queueName)
            ? throw new ArgumentException("Queue name must be provided.", nameof(queueName))
            : queueName;
        _logger = logger ?? NullLogger<RmqConsumer<T>>.Instance;
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycleLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_isStarted && _channel is { IsOpen: true })
            {
                return;
            }

            await _connectionManager.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
            var channel = await _connectionManager.CreateChannelAsync(cancellationToken).ConfigureAwait(false);

            var queueOptions = GetQueueOptions(_queueName);
            await _queueManager.DeclareQueueWithDlqAsync(channel, _queueName, queueOptions, cancellationToken).ConfigureAwait(false);

            if (queueOptions.PrefetchCount > 0)
            {
                await channel.BasicQosAsync(0, queueOptions.PrefetchCount, false, cancellationToken).ConfigureAwait(false);
            }

            var consumerHandler = new RmqAsyncConsumerHandler<T>(
                channel,
                _messageHandlerInvoker,
                _cloudEventWrapper,
                queueOptions.Retry,
                _queueName,
                _logger);

            var consumerTag = await channel.BasicConsumeAsync(
                queue: _queueName,
                autoAck: false,
                consumerTag: string.Empty,
                noLocal: false,
                exclusive: false,
                arguments: null,
                consumer: consumerHandler,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            _channel = channel;
            _consumerTag = consumerTag;
            _isStarted = true;

            _logger.LogInformation("Consumer iniciado para queue {QueueName} com tag {ConsumerTag}", _queueName, _consumerTag);
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycleLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!_isStarted || _channel is null)
            {
                return;
            }

            var channel = _channel;
            var consumerTag = _consumerTag;

            _channel = null;
            _consumerTag = null;
            _isStarted = false;

            if (!string.IsNullOrWhiteSpace(consumerTag))
            {
                await channel.BasicCancelAsync(consumerTag, false, cancellationToken).ConfigureAwait(false);
            }

            if (channel.IsOpen)
            {
                await channel.CloseAsync(
                    replyCode: 200,
                    replyText: "Consumer stopped",
                    abort: false,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            await channel.DisposeAsync().ConfigureAwait(false);

            _logger.LogInformation("Consumer parado para queue {QueueName}", _queueName);
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _lifecycleLock.Dispose();
    }

    private QueueOptions GetQueueOptions(string queueName)
    {
        if (_options.Queues.TryGetValue(queueName, out var queueOptions))
        {
            return queueOptions;
        }

        return new QueueOptions
        {
            PrefetchCount = 0,
            Retry = new RetryOptions
            {
                MaxAttempts = _options.DefaultRetry.MaxAttempts,
                InitialDelay = _options.DefaultRetry.InitialDelay,
                BackoffType = _options.DefaultRetry.BackoffType,
                UseJitter = _options.DefaultRetry.UseJitter
            },
            Dlq = new DlqOptions
            {
                Enabled = true,
                QueueNameSuffix = ".dlq"
            }
        };
    }
}
