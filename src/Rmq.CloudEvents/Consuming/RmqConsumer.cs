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
    private readonly IRmqMessageHandler<T> _messageHandler;
    private readonly RmqOptions _options;
    private readonly string _queueName;
    private readonly ILogger<RmqConsumer<T>> _logger;
    private IChannel? _channel;
    private string? _consumerTag;

    /// <summary>
    /// Inicializa uma nova instancia de <see cref="RmqConsumer{T}"/>.
    /// </summary>
    public RmqConsumer(
        IRmqConnectionManager connectionManager,
        IQueueManager queueManager,
        ICloudEventWrapper cloudEventWrapper,
        IRmqMessageHandler<T> messageHandler,
        RmqOptions options,
        string queueName,
        ILogger<RmqConsumer<T>>? logger = null)
    {
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _queueManager = queueManager ?? throw new ArgumentNullException(nameof(queueManager));
        _cloudEventWrapper = cloudEventWrapper ?? throw new ArgumentNullException(nameof(cloudEventWrapper));
        _messageHandler = messageHandler ?? throw new ArgumentNullException(nameof(messageHandler));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _queueName = string.IsNullOrWhiteSpace(queueName)
            ? throw new ArgumentException("Queue name must be provided.", nameof(queueName))
            : queueName;
        _logger = logger ?? NullLogger<RmqConsumer<T>>.Instance;
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_channel is { IsOpen: true })
        {
            return;
        }

        _channel = await _connectionManager.CreateChannelAsync(cancellationToken).ConfigureAwait(false);

        var queueOptions = GetQueueOptions(_queueName);
        await _queueManager.DeclareQueueWithDlqAsync(_channel, _queueName, queueOptions, cancellationToken).ConfigureAwait(false);

        var consumerHandler = new RmqAsyncConsumerHandler<T>(
            _channel,
            _messageHandler,
            _cloudEventWrapper,
            queueOptions.Retry,
            _queueName,
            _logger);

        _consumerTag = await _channel.BasicConsumeAsync(
            queue: _queueName,
            autoAck: false,
            consumerTag: string.Empty,
            noLocal: false,
            exclusive: false,
            arguments: null,
            consumer: consumerHandler,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("Consumer iniciado para queue {QueueName} com tag {ConsumerTag}", _queueName, _consumerTag);
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_channel is null)
        {
            return;
        }

        if (!string.IsNullOrWhiteSpace(_consumerTag))
        {
            await _channel.BasicCancelAsync(_consumerTag, false, cancellationToken).ConfigureAwait(false);
        }

        await _channel.DisposeAsync().ConfigureAwait(false);
        _channel = null;
        _consumerTag = null;

        _logger.LogInformation("Consumer parado para queue {QueueName}", _queueName);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
    }

    private QueueOptions GetQueueOptions(string queueName)
    {
        if (_options.Queues.TryGetValue(queueName, out var queueOptions))
        {
            return queueOptions;
        }

        return new QueueOptions
        {
            Retry = new RetryOptions
            {
                MaxAttempts = _options.DefaultRetry.MaxAttempts,
                InitialDelay = _options.DefaultRetry.InitialDelay,
                BackoffType = _options.DefaultRetry.BackoffType,
                UseJitter = _options.DefaultRetry.UseJitter
            }
        };
    }
}
