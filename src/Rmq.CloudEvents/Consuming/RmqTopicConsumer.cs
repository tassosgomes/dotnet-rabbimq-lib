using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Rmq.CloudEvents.CloudEvents;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Connection;
using Rmq.CloudEvents.Infrastructure;

namespace Rmq.CloudEvents.Consuming;

/// <summary>
/// Hosted service para consumo de mensagens via Topic Exchange.
/// </summary>
/// <typeparam name="T">Tipo do payload consumido.</typeparam>
internal sealed class RmqTopicConsumer<T> : RmqConsumerHostedServiceBase<T>
    where T : class
{
    private readonly IQueueManager _queueManager;
    private readonly RmqOptions _options;
    private readonly TopicSubscriptionOptions _subscription;
    private readonly ILogger<RmqTopicConsumer<T>> _logger;

    public RmqTopicConsumer(
        IRmqConnectionManager connectionManager,
        IQueueManager queueManager,
        ICloudEventWrapper cloudEventWrapper,
        Func<T, MessageContext, CancellationToken, Task> messageHandlerInvoker,
        RmqOptions options,
        TopicSubscriptionOptions subscription,
        ILogger<RmqTopicConsumer<T>>? logger = null)
        : base(
            connectionManager,
            cloudEventWrapper,
            messageHandlerInvoker,
            logger ?? NullLogger<RmqTopicConsumer<T>>.Instance)
    {
        _queueManager = queueManager ?? throw new ArgumentNullException(nameof(queueManager));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _subscription = subscription ?? throw new ArgumentNullException(nameof(subscription));
        _logger = logger ?? NullLogger<RmqTopicConsumer<T>>.Instance;
    }

    protected override string QueueName =>
        _subscription.QueueName
        ?? throw new InvalidOperationException("QueueName is required for durable topic consumers.");

    protected override RetryOptions RetryOptions => _subscription.Queue.Retry;

    protected override ushort PrefetchCount => _subscription.Queue.PrefetchCount;

    protected override TimeSpan RecoveryDelay => _options.Connection.NetworkRecoveryInterval;

    protected override string StopReplyText => "Topic consumer stopped";

    protected override Task DeclareTopologyAsync(IChannel channel, CancellationToken cancellationToken)
    {
        var exchangeOptions = _options.Exchanges.TryGetValue(_subscription.ExchangeName, out var opts)
            ? opts
            : null;

        return _queueManager.DeclareExchangeAndBindingsAsync(
            channel,
            _subscription.ExchangeName,
            QueueName,
            _subscription.BindingPatterns,
            _subscription.Queue,
            exchangeOptions,
            cancellationToken);
    }

    protected override void LogConsumerStarted(string consumerTag) =>
        _logger.LogInformation(
            "Topic consumer iniciado. Exchange={Exchange}, Queue={Queue}, Patterns=[{Patterns}], Tag={Tag}",
            _subscription.ExchangeName,
            QueueName,
            string.Join(", ", _subscription.BindingPatterns),
            consumerTag);

    protected override void LogConsumerStopped() =>
        _logger.LogInformation(
            "Topic consumer parado. Exchange={Exchange}, Queue={Queue}",
            _subscription.ExchangeName,
            _subscription.QueueName);

    protected override void LogRecoveryTriggered(object? signal) =>
        _logger.LogWarning(
            "Topic consumer da exchange {Exchange} e queue {Queue} sinalizou recuperacao apos evento {SignalType}.",
            _subscription.ExchangeName,
            QueueName,
            signal?.GetType().Name ?? "Unknown");
}
