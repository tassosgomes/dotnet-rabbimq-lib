using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Rmq.CloudEvents.CloudEvents;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Connection;
using Rmq.CloudEvents.Diagnostics;
using Rmq.CloudEvents.Exceptions;
using Rmq.CloudEvents.Infrastructure;
using System.Collections.Concurrent;
using System.Diagnostics;

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
    private readonly SemaphoreSlim _topologyLock = new(1, 1);
    private readonly ConcurrentDictionary<string, byte> _declaredQueues = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _declaredExchanges = new(StringComparer.Ordinal);

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

    public Task PublishAsync<T>(
        string queueName,
        T payload,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        return PublishInternalAsync(queueName, payload, headers: null, cloudEventType, cancellationToken);
    }

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

    public Task PublishToTopicAsync<T>(
        string exchangeName,
        string routingKey,
        T payload,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        return PublishToTopicInternalAsync(exchangeName, routingKey, payload, headers: null, cloudEventType, cancellationToken);
    }

    public Task PublishToTopicAsync<T>(
        string exchangeName,
        string routingKey,
        T payload,
        IDictionary<string, object> headers,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(headers);
        return PublishToTopicInternalAsync(exchangeName, routingKey, payload, headers, cloudEventType, cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        _topologyLock.Dispose();
        return ValueTask.CompletedTask;
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

        var body = _cloudEventWrapper.Wrap(payload, cloudEventType);
        var queueOptions = GetQueueOptions(queueName);
        var retryPipeline = BuildRetryPipeline(queueOptions.Retry, _logger, "queue", queueName);
        var stopwatch = Stopwatch.StartNew();

        using var activity = RmqDiagnostics.StartPublishActivity("queue", queueName, queueName);
        try
        {
            await retryPipeline.ExecuteAsync(async ct =>
            {
                try
                {
                    await using var channel = await _connectionManager.CreatePublisherChannelAsync(ct).ConfigureAwait(false);
                    await EnsureQueueDeclaredAsync(channel, queueName, queueOptions, ct).ConfigureAwait(false);

                    var properties = CreateProperties(headers);
                    activity?.SetTag("messaging.message.id", properties.MessageId);
                    RmqDiagnostics.RecordPublishAttempt("queue", queueName);

                    using var scope = _logger.BeginScope(new Dictionary<string, object?>
                    {
                        ["RmqDestinationKind"] = "queue",
                        ["RmqDestinationName"] = queueName,
                        ["RmqMessageId"] = properties.MessageId
                    });

                    await PublishAndConfirmAsync(
                        channel,
                        exchange: string.Empty,
                        routingKey: queueName,
                        mandatory: true,
                        properties: properties,
                        body: body,
                        cancellationToken: ct).ConfigureAwait(false);

                    _logger.LogDebug("Mensagem publicada com sucesso na queue {QueueName}", queueName);
                }
                catch
                {
                    _declaredQueues.TryRemove(queueName, out _);
                    throw;
                }
            }, cancellationToken).ConfigureAwait(false);

            stopwatch.Stop();
            activity?.SetStatus(ActivityStatusCode.Ok);
            RmqDiagnostics.RecordPublishSuccess("queue", queueName, stopwatch.Elapsed.TotalMilliseconds);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("exception.type", ex.GetType().FullName);
            activity?.SetTag("exception.message", ex.Message);
            RmqDiagnostics.RecordPublishFailure("queue", queueName, stopwatch.Elapsed.TotalMilliseconds);
            _declaredQueues.TryRemove(queueName, out _);
            _logger.LogError(ex, "Falha ao publicar mensagem na queue {QueueName} apos retries", queueName);
            throw new RmqPublishException(queueName, queueOptions.Retry.MaxAttempts, ex);
        }
    }

    private async Task PublishToTopicInternalAsync<T>(
        string exchangeName,
        string routingKey,
        T payload,
        IDictionary<string, object>? headers,
        string? cloudEventType,
        CancellationToken cancellationToken)
        where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(exchangeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        ArgumentNullException.ThrowIfNull(payload);

        var body = _cloudEventWrapper.Wrap(payload, cloudEventType);
        var retryPipeline = BuildRetryPipeline(_options.DefaultRetry, _logger, "exchange", exchangeName);
        var stopwatch = Stopwatch.StartNew();

        using var activity = RmqDiagnostics.StartPublishActivity("exchange", exchangeName, routingKey);
        try
        {
            await retryPipeline.ExecuteAsync(async ct =>
            {
                try
                {
                    await using var channel = await _connectionManager.CreatePublisherChannelAsync(ct).ConfigureAwait(false);
                    await EnsureExchangeDeclaredAsync(channel, exchangeName, ct).ConfigureAwait(false);

                    var properties = CreateProperties(headers);
                    activity?.SetTag("messaging.message.id", properties.MessageId);
                    RmqDiagnostics.RecordPublishAttempt("exchange", exchangeName);

                    using var scope = _logger.BeginScope(new Dictionary<string, object?>
                    {
                        ["RmqDestinationKind"] = "exchange",
                        ["RmqDestinationName"] = exchangeName,
                        ["RmqRoutingKey"] = routingKey,
                        ["RmqMessageId"] = properties.MessageId
                    });

                    await PublishAndConfirmAsync(
                        channel,
                        exchange: exchangeName,
                        routingKey: routingKey,
                        mandatory: true,
                        properties: properties,
                        body: body,
                        cancellationToken: ct).ConfigureAwait(false);

                    _logger.LogDebug(
                        "Mensagem publicada com sucesso na exchange {ExchangeName} com routing key {RoutingKey}",
                        exchangeName,
                        routingKey);
                }
                catch
                {
                    _declaredExchanges.TryRemove(exchangeName, out _);
                    throw;
                }
            }, cancellationToken).ConfigureAwait(false);

            stopwatch.Stop();
            activity?.SetStatus(ActivityStatusCode.Ok);
            RmqDiagnostics.RecordPublishSuccess("exchange", exchangeName, stopwatch.Elapsed.TotalMilliseconds);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("exception.type", ex.GetType().FullName);
            activity?.SetTag("exception.message", ex.Message);
            RmqDiagnostics.RecordPublishFailure("exchange", exchangeName, stopwatch.Elapsed.TotalMilliseconds);
            _declaredExchanges.TryRemove(exchangeName, out _);
            _logger.LogError(
                ex,
                "Falha ao publicar mensagem na exchange {ExchangeName} com routing key {RoutingKey} apos retries",
                exchangeName,
                routingKey);
            throw new RmqPublishException($"{exchangeName}/{routingKey}", _options.DefaultRetry.MaxAttempts, ex);
        }
    }

    private static BasicProperties CreateProperties(IDictionary<string, object>? headers)
    {
        return new BasicProperties
        {
            ContentType = "application/cloudevents+json",
            DeliveryMode = DeliveryModes.Persistent,
            MessageId = Guid.NewGuid().ToString(),
            Headers = headers is null
                ? null
                : headers.ToDictionary(kvp => kvp.Key, kvp => (object?)kvp.Value)
        };
    }

    private async Task EnsureQueueDeclaredAsync(
        IChannel channel,
        string queueName,
        QueueOptions queueOptions,
        CancellationToken cancellationToken)
    {
        if (_declaredQueues.ContainsKey(queueName))
        {
            return;
        }

        await _topologyLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_declaredQueues.ContainsKey(queueName))
            {
                return;
            }

            await _queueManager.DeclareQueueWithDlqAsync(channel, queueName, queueOptions, cancellationToken).ConfigureAwait(false);
            _declaredQueues.TryAdd(queueName, 0);
        }
        finally
        {
            _topologyLock.Release();
        }
    }

    private async Task EnsureExchangeDeclaredAsync(
        IChannel channel,
        string exchangeName,
        CancellationToken cancellationToken)
    {
        if (_declaredExchanges.ContainsKey(exchangeName))
        {
            return;
        }

        await _topologyLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_declaredExchanges.ContainsKey(exchangeName))
            {
                return;
            }

            var opts = _options.Exchanges.TryGetValue(exchangeName, out var configured)
                ? configured
                : new ExchangeOptions { Name = exchangeName };

            await channel.ExchangeDeclareAsync(
                exchange: exchangeName,
                type: ExchangeType.Topic,
                durable: opts.Durable,
                autoDelete: opts.AutoDelete,
                arguments: opts.Arguments?.ToDictionary(kvp => kvp.Key, kvp => (object?)kvp.Value),
                cancellationToken: cancellationToken).ConfigureAwait(false);

            _declaredExchanges.TryAdd(exchangeName, 0);
        }
        finally
        {
            _topologyLock.Release();
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

    private static ResiliencePipeline BuildRetryPipeline(
        RetryOptions options,
        ILogger logger,
        string destinationKind,
        string destinationName)
    {
        var totalAttempts = Math.Max(1, options.MaxAttempts);

        if (totalAttempts == 1)
        {
            return new ResiliencePipelineBuilder().Build();
        }

        return new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder()
                    .Handle<RabbitMQClientException>()
                    .Handle<IOException>()
                    .Handle<TimeoutException>(),
                MaxRetryAttempts = totalAttempts - 1,
                Delay = options.InitialDelay,
                BackoffType = ToDelayBackoffType(options.BackoffType),
                UseJitter = options.UseJitter,
                OnRetry = args =>
                {
                    RmqDiagnostics.RecordPublishRetry(destinationKind, destinationName);
                    logger.LogWarning(
                        args.Outcome.Exception,
                        "Tentativa de publish {Attempt}/{MaxAttempts} falhou para {DestinationKind} {DestinationName}. Proximo retry em {RetryDelay}",
                        args.AttemptNumber + 2,
                        totalAttempts,
                        destinationKind,
                        destinationName,
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

    private async Task PublishAndConfirmAsync(
        IChannel channel,
        string exchange,
        string routingKey,
        bool mandatory,
        BasicProperties properties,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken)
    {
        var confirmation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        AsyncEventHandler<BasicAckEventArgs> onAck = (_, _) =>
        {
            confirmation.TrySetResult();
            return Task.CompletedTask;
        };

        AsyncEventHandler<BasicNackEventArgs> onNack = (_, args) =>
        {
            confirmation.TrySetException(
                new InvalidOperationException(
                    $"Broker negatively acknowledged publish to '{DescribeTarget(exchange, routingKey)}' (deliveryTag={args.DeliveryTag})."));
            return Task.CompletedTask;
        };

        AsyncEventHandler<BasicReturnEventArgs> onReturn = (_, args) =>
        {
            var returnedReason = $"{args.ReplyCode} {args.ReplyText}".Trim();
            confirmation.TrySetException(
                new InvalidOperationException(
                    $"Broker returned unroutable publish to '{DescribeTarget(exchange, routingKey)}' ({returnedReason})."));
            return Task.CompletedTask;
        };

        channel.BasicAcksAsync += onAck;
        channel.BasicNacksAsync += onNack;
        channel.BasicReturnAsync += onReturn;

        try
        {
            using var timeoutCts = new CancellationTokenSource(_options.PublishConfirmTimeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            await channel.BasicPublishAsync(
                exchange: exchange,
                routingKey: routingKey,
                mandatory: mandatory,
                basicProperties: properties,
                body: body,
                cancellationToken: linkedCts.Token).ConfigureAwait(false);

            using var registration = linkedCts.Token.Register(() =>
            {
                if (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                {
                    confirmation.TrySetException(
                        new TimeoutException(
                            $"Timed out after {_options.PublishConfirmTimeout} waiting for broker confirmation for '{DescribeTarget(exchange, routingKey)}'."));
                    return;
                }

                confirmation.TrySetCanceled(linkedCts.Token);
            });

            await confirmation.Task.ConfigureAwait(false);
        }
        finally
        {
            channel.BasicAcksAsync -= onAck;
            channel.BasicNacksAsync -= onNack;
            channel.BasicReturnAsync -= onReturn;
        }
    }

    private static string DescribeTarget(string exchange, string routingKey)
    {
        return string.IsNullOrWhiteSpace(exchange)
            ? routingKey
            : $"{exchange}/{routingKey}";
    }
}
