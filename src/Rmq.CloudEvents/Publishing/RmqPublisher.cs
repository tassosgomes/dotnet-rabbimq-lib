using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Rmq.CloudEvents.CloudEvents;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Connection;
using Rmq.CloudEvents.Exceptions;
using Rmq.CloudEvents.Infrastructure;

namespace Rmq.CloudEvents.Publishing;

/// <summary>
/// Publicador de mensagens com retry exponencial e CloudEvents transparente.
/// </summary>
internal sealed class RmqPublisher : IRmqPublisher
{
    private readonly IRmqConnectionManager _connectionManager;
    private readonly IQueueManager _queueManager;
    private readonly ICloudEventWrapper _cloudEventWrapper;
    private readonly RmqOptions _options;
    private readonly ILogger<RmqPublisher> _logger;
    private readonly SemaphoreSlim _channelLock = new(1, 1);
    private readonly HashSet<string> _declaredQueues = new(StringComparer.Ordinal);
    private IChannel? _channel;

    /// <summary>
    /// Inicializa uma nova instancia de <see cref="RmqPublisher"/>.
    /// </summary>
    public RmqPublisher(
        IRmqConnectionManager connectionManager,
        IQueueManager queueManager,
        ICloudEventWrapper cloudEventWrapper,
        RmqOptions options,
        ILogger<RmqPublisher>? logger = null)
    {
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _queueManager = queueManager ?? throw new ArgumentNullException(nameof(queueManager));
        _cloudEventWrapper = cloudEventWrapper ?? throw new ArgumentNullException(nameof(cloudEventWrapper));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? NullLogger<RmqPublisher>.Instance;
    }

    /// <inheritdoc />
    public Task PublishAsync<T>(
        string queueName,
        T payload,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        return PublishInternalAsync(queueName, payload, headers: null, cloudEventType, cancellationToken);
    }

    /// <inheritdoc />
    public Task PublishAsync<T>(
        string queueName,
        T payload,
        IDictionary<string, object> headers,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(headers);
        return PublishInternalAsync(queueName, payload, headers, cloudEventType, cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            await _channel.DisposeAsync().ConfigureAwait(false);
            _channel = null;
        }

        _channelLock.Dispose();
    }

    private async Task PublishInternalAsync<T>(
        string queueName,
        T payload,
        IDictionary<string, object>? headers,
        string? cloudEventType,
        CancellationToken cancellationToken)
        where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        ArgumentNullException.ThrowIfNull(payload);

        await EnsureChannelAsync(cancellationToken).ConfigureAwait(false);
        await EnsureQueueDeclaredAsync(queueName, cancellationToken).ConfigureAwait(false);

        var body = _cloudEventWrapper.Wrap(payload, cloudEventType);
        var queueOptions = GetQueueOptions(queueName);
        var retryPipeline = BuildRetryPipeline(queueOptions.Retry, _logger);

        try
        {
            await retryPipeline.ExecuteAsync(async ct =>
            {
                var properties = new BasicProperties
                {
                    ContentType = "application/cloudevents+json",
                    DeliveryMode = DeliveryModes.Persistent,
                    MessageId = Guid.NewGuid().ToString(),
                    Headers = headers is null
                        ? null
                        : headers.ToDictionary(kvp => kvp.Key, kvp => (object?)kvp.Value)
                };

                await _channel!.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: queueName,
                    mandatory: true,
                    basicProperties: properties,
                    body: body,
                    cancellationToken: ct).ConfigureAwait(false);

                _logger.LogDebug("Mensagem publicada com sucesso na queue {QueueName}", queueName);
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha ao publicar mensagem na queue {QueueName} apos retries", queueName);
            throw new RmqPublishException(queueName, queueOptions.Retry.MaxAttempts, ex);
        }
    }

    private async Task EnsureChannelAsync(CancellationToken cancellationToken)
    {
        if (_channel is { IsOpen: true })
        {
            return;
        }

        await _channelLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_channel is { IsOpen: true })
            {
                return;
            }

            _channel = await _connectionManager.CreateChannelAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _channelLock.Release();
        }
    }

    private async Task EnsureQueueDeclaredAsync(string queueName, CancellationToken cancellationToken)
    {
        if (_declaredQueues.Contains(queueName))
        {
            return;
        }

        await _channelLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_declaredQueues.Contains(queueName))
            {
                return;
            }

            await _queueManager.DeclareQueueWithDlqAsync(
                _channel!,
                queueName,
                GetQueueOptions(queueName),
                cancellationToken).ConfigureAwait(false);

            _declaredQueues.Add(queueName);
        }
        finally
        {
            _channelLock.Release();
        }
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

    private static ResiliencePipeline BuildRetryPipeline(RetryOptions options, ILogger logger)
    {
        return new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder()
                    .Handle<RabbitMQClientException>()
                    .Handle<IOException>()
                    .Handle<TimeoutException>(),
                MaxRetryAttempts = options.MaxAttempts,
                Delay = options.InitialDelay,
                BackoffType = ToDelayBackoffType(options.BackoffType),
                UseJitter = options.UseJitter,
                OnRetry = args =>
                {
                    logger.LogWarning(
                        args.Outcome.Exception,
                        "Tentativa de publish {Attempt}/{MaxAttempts} falhou. Proximo retry em {RetryDelay}",
                        args.AttemptNumber + 1,
                        options.MaxAttempts,
                        args.RetryDelay);

                    return default;
                }
            })
            .Build();
    }

    private static DelayBackoffType ToDelayBackoffType(BackoffType backoffType)
    {
        return backoffType switch
        {
            BackoffType.Linear => DelayBackoffType.Linear,
            BackoffType.Constant => DelayBackoffType.Constant,
            _ => DelayBackoffType.Exponential
        };
    }
}
