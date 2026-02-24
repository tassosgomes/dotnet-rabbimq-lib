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
/// Hosted service para consumo de mensagens via Topic Exchange.
/// </summary>
/// <typeparam name="T">Tipo do payload consumido.</typeparam>
internal sealed class RmqTopicConsumer<T> : IHostedService, IRmqConsumer
    where T : class
{
    private readonly IRmqConnectionManager _connectionManager;
    private readonly IQueueManager _queueManager;
    private readonly ICloudEventWrapper _cloudEventWrapper;
    private readonly IRmqMessageHandler<T> _messageHandler;
    private readonly RmqOptions _options;
    private readonly TopicSubscriptionOptions _subscription;
    private readonly ILogger<RmqTopicConsumer<T>> _logger;
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);
    private IChannel? _channel;
    private string? _consumerTag;
    private bool _isStarted;
    private bool _disposed;

    public RmqTopicConsumer(
        IRmqConnectionManager connectionManager,
        IQueueManager queueManager,
        ICloudEventWrapper cloudEventWrapper,
        IRmqMessageHandler<T> messageHandler,
        RmqOptions options,
        TopicSubscriptionOptions subscription,
        ILogger<RmqTopicConsumer<T>>? logger = null)
    {
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _queueManager = queueManager ?? throw new ArgumentNullException(nameof(queueManager));
        _cloudEventWrapper = cloudEventWrapper ?? throw new ArgumentNullException(nameof(cloudEventWrapper));
        _messageHandler = messageHandler ?? throw new ArgumentNullException(nameof(messageHandler));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _subscription = subscription ?? throw new ArgumentNullException(nameof(subscription));
        _logger = logger ?? NullLogger<RmqTopicConsumer<T>>.Instance;
    }

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

            var queueName = _subscription.QueueName
                ?? throw new InvalidOperationException("QueueName is required for durable topic consumers.");

            var exchangeOptions = _options.Exchanges.TryGetValue(_subscription.ExchangeName, out var opts)
                ? opts
                : null;

            await _queueManager.DeclareExchangeAndBindingsAsync(
                channel,
                _subscription.ExchangeName,
                queueName,
                _subscription.BindingPatterns,
                _subscription.Queue,
                exchangeOptions,
                cancellationToken).ConfigureAwait(false);

            var retryOptions = _subscription.Queue.Retry;

            var consumerHandler = new RmqAsyncConsumerHandler<T>(
                channel,
                _messageHandler,
                _cloudEventWrapper,
                retryOptions,
                queueName,
                _logger);

            var consumerTag = await channel.BasicConsumeAsync(
                queue: queueName,
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

            _logger.LogInformation(
                "Topic consumer iniciado. Exchange={Exchange}, Queue={Queue}, Patterns=[{Patterns}], Tag={Tag}",
                _subscription.ExchangeName,
                queueName,
                string.Join(", ", _subscription.BindingPatterns),
                _consumerTag);
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed)
        {
            return;
        }

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
                await channel.BasicCancelAsync(consumerTag, false, cancellationToken)
                    .ConfigureAwait(false);
            }

            if (channel.IsOpen)
            {
                await channel.CloseAsync(200, "Topic consumer stopped", false, cancellationToken)
                    .ConfigureAwait(false);
            }

            await channel.DisposeAsync().ConfigureAwait(false);

            _logger.LogInformation(
                "Topic consumer parado. Exchange={Exchange}, Queue={Queue}",
                _subscription.ExchangeName,
                _subscription.QueueName);
        }
        finally
        {
            _lifecycleLock.Release();
        }
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
}
