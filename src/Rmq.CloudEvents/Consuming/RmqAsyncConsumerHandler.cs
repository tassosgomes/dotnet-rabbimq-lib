using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using Rmq.CloudEvents.CloudEvents;
using Rmq.CloudEvents.Configuration;

namespace Rmq.CloudEvents.Consuming;

/// <summary>
/// Consumer assincrono que processa mensagens com retry e ACK/NACK.
/// </summary>
/// <typeparam name="T">Tipo do payload.</typeparam>
internal sealed class RmqAsyncConsumerHandler<T> : AsyncDefaultBasicConsumer
    where T : class
{
    private readonly IRmqMessageHandler<T> _handler;
    private readonly ICloudEventWrapper _cloudEventWrapper;
    private readonly ResiliencePipeline _retryPipeline;
    private readonly string _queueName;
    private readonly ILogger _logger;

    /// <summary>
    /// Inicializa uma nova instancia de <see cref="RmqAsyncConsumerHandler{T}"/>.
    /// </summary>
    public RmqAsyncConsumerHandler(
        IChannel channel,
        IRmqMessageHandler<T> handler,
        ICloudEventWrapper cloudEventWrapper,
        RetryOptions retryOptions,
        string queueName,
        ILogger? logger = null)
        : base(channel)
    {
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        _cloudEventWrapper = cloudEventWrapper ?? throw new ArgumentNullException(nameof(cloudEventWrapper));
        _queueName = string.IsNullOrWhiteSpace(queueName)
            ? throw new ArgumentException("Queue name must be provided.", nameof(queueName))
            : queueName;
        _logger = logger ?? NullLogger.Instance;
        _retryPipeline = BuildRetryPipeline(
            retryOptions ?? throw new ArgumentNullException(nameof(retryOptions)),
            _logger,
            _queueName);
    }

    /// <inheritdoc />
    public override async Task HandleBasicDeliverAsync(
        string consumerTag,
        ulong deliveryTag,
        bool redelivered,
        string exchange,
        string routingKey,
        IReadOnlyBasicProperties properties,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var (payload, metadata) = _cloudEventWrapper.Unwrap<T>(body);
            var headers = ConvertHeaders(properties.Headers);
            var currentAttempt = 0;

            await _retryPipeline.ExecuteAsync(
                async ct =>
                {
                    currentAttempt++;

                    var context = CreateMessageContext(
                        metadata,
                        headers,
                        deliveryTag,
                        currentAttempt,
                        redelivered);

                    await _handler.HandleAsync(payload, context, ct).ConfigureAwait(false);
                },
                cancellationToken).ConfigureAwait(false);

            await Channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug(
                "Mensagem {EventId} processada com sucesso da queue {QueueName} na tentativa {AttemptNumber}",
                metadata.EventId,
                _queueName,
                currentAttempt);
        }
        catch (OperationCanceledException ex)
        {
            _logger.LogInformation(
                ex,
                "Processamento da mensagem {DeliveryTag} cancelado na queue {QueueName}. ACK/NACK nao sera enviado.",
                deliveryTag,
                _queueName);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Mensagem {DeliveryTag} falhou apos retries na queue {QueueName}. Enviando para DLQ.",
                deliveryTag,
                _queueName);
            await Channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false, cancellationToken).ConfigureAwait(false);
        }
    }

    private static ResiliencePipeline BuildRetryPipeline(RetryOptions options, ILogger logger, string queueName)
    {
        var totalAttempts = Math.Max(1, options.MaxAttempts);

        if (totalAttempts == 1)
        {
            return new ResiliencePipelineBuilder().Build();
        }

        return new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = totalAttempts - 1,
                Delay = options.InitialDelay,
                BackoffType = options.BackoffType switch
                {
                    BackoffType.Linear => DelayBackoffType.Linear,
                    BackoffType.Constant => DelayBackoffType.Constant,
                    _ => DelayBackoffType.Exponential
                },
                UseJitter = options.UseJitter,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(ex => ex is not OperationCanceledException),
                OnRetry = args =>
                {
                    logger.LogWarning(
                        args.Outcome.Exception,
                        "Tentativa de consume {Attempt}/{MaxAttempts} falhou para queue {QueueName}. Proximo retry em {RetryDelay}",
                        args.AttemptNumber + 2,
                        totalAttempts,
                        queueName,
                        args.RetryDelay);

                    return default;
                }
            })
            .Build();
    }

    private MessageContext CreateMessageContext(
        CloudEventMetadata metadata,
        IReadOnlyDictionary<string, object> headers,
        ulong deliveryTag,
        int currentAttempt,
        bool redelivered)
    {
        var initialAttempt = redelivered ? 2 : 1;

        return new MessageContext
        {
            EventId = metadata.EventId,
            Source = metadata.Source,
            EventType = metadata.EventType,
            Timestamp = metadata.Timestamp,
            Headers = headers,
            DeliveryTag = deliveryTag,
            QueueName = _queueName,
            AttemptNumber = (initialAttempt - 1) + currentAttempt
        };
    }

    private static IReadOnlyDictionary<string, object> ConvertHeaders(IDictionary<string, object?>? headers)
    {
        if (headers is null || headers.Count == 0)
        {
            return new Dictionary<string, object>();
        }

        return headers.ToDictionary(kvp => kvp.Key, kvp => kvp.Value ?? string.Empty);
    }
}
