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
internal sealed class RmqConsumer<T> : RmqConsumerHostedServiceBase<T>
    where T : class
{
    private readonly IQueueManager _queueManager;
    private readonly RmqOptions _options;
    private readonly string _queueName;
    private readonly ILogger<RmqConsumer<T>> _logger;
    private readonly QueueOptions _queueOptions;

    public RmqConsumer(
        IRmqConnectionManager connectionManager,
        IQueueManager queueManager,
        ICloudEventWrapper cloudEventWrapper,
        Func<T, MessageContext, CancellationToken, Task> messageHandlerInvoker,
        RmqOptions options,
        string queueName,
        ILogger<RmqConsumer<T>>? logger = null)
        : base(
            connectionManager,
            cloudEventWrapper,
            messageHandlerInvoker,
            logger ?? NullLogger<RmqConsumer<T>>.Instance)
    {
        _queueManager = queueManager ?? throw new ArgumentNullException(nameof(queueManager));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _queueName = string.IsNullOrWhiteSpace(queueName)
            ? throw new ArgumentException("Queue name must be provided.", nameof(queueName))
            : queueName;
        _logger = logger ?? NullLogger<RmqConsumer<T>>.Instance;
        _queueOptions = GetQueueOptions(_queueName);
    }

    protected override string QueueName => _queueName;

    protected override RetryOptions RetryOptions => _queueOptions.Retry;

    protected override ushort PrefetchCount => _queueOptions.PrefetchCount;

    protected override TimeSpan RecoveryDelay => _options.Connection.NetworkRecoveryInterval;

    protected override string StopReplyText => "Consumer stopped";

    protected override Task DeclareTopologyAsync(IChannel channel, CancellationToken cancellationToken) =>
        _queueManager.DeclareQueueWithDlqAsync(channel, _queueName, _queueOptions, cancellationToken);

    protected override void LogConsumerStarted(string consumerTag) =>
        _logger.LogInformation("Consumer iniciado para queue {QueueName} com tag {ConsumerTag}", _queueName, consumerTag);

    protected override void LogConsumerStopped() =>
        _logger.LogInformation("Consumer parado para queue {QueueName}", _queueName);

    protected override void LogRecoveryTriggered(object? signal) =>
        _logger.LogWarning("Consumer da queue {QueueName} sinalizou recuperacao apos evento {SignalType}.", _queueName, signal?.GetType().Name ?? "Unknown");

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
