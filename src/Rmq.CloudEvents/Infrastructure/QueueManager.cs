using RabbitMQ.Client;
using Rmq.CloudEvents.Configuration;

namespace Rmq.CloudEvents.Infrastructure;

/// <summary>
/// Implementacao de declaracao de quorum queues e DLQ.
/// </summary>
internal sealed class QueueManager : IQueueManager
{
    /// <inheritdoc />
    public async Task DeclareQueueWithDlqAsync(
        IChannel channel,
        string queueName,
        QueueOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        ArgumentNullException.ThrowIfNull(options);

        var dlqName = $"{queueName}{options.Dlq.QueueNameSuffix}";
        var dlxName = $"{queueName}.dlx";

        await channel.ExchangeDeclareAsync(
            exchange: dlxName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        var dlqArguments = new Dictionary<string, object?>
        {
            ["x-queue-type"] = "quorum"
        };

        await channel.QueueDeclareAsync(
            queue: dlqName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: dlqArguments,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await channel.QueueBindAsync(
            queue: dlqName,
            exchange: dlxName,
            routingKey: queueName,
            arguments: null,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        var queueArguments = new Dictionary<string, object?>
        {
            ["x-queue-type"] = "quorum",
            ["x-dead-letter-exchange"] = dlxName,
            ["x-dead-letter-routing-key"] = queueName,
            ["x-delivery-limit"] = options.DeliveryLimit
        };

        if (options.QuorumSize > 0)
        {
            queueArguments["x-quorum-initial-group-size"] = options.QuorumSize;
        }

        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArguments,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task DeclareExchangeAndBindingsAsync(
        IChannel channel,
        string exchangeName,
        string queueName,
        IReadOnlyList<string> bindingPatterns,
        QueueOptions queueOptions,
        ExchangeOptions? exchangeOptions = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentException.ThrowIfNullOrWhiteSpace(exchangeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        ArgumentNullException.ThrowIfNull(bindingPatterns);

        if (bindingPatterns.Count == 0)
        {
            throw new ArgumentException("At least one binding pattern is required.", nameof(bindingPatterns));
        }

        // 1. Declarar a Topic Exchange
        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Topic,
            durable: exchangeOptions?.Durable ?? true,
            autoDelete: exchangeOptions?.AutoDelete ?? false,
            arguments: exchangeOptions?.Arguments?.ToDictionary(
                kvp => kvp.Key, kvp => (object?)kvp.Value),
            cancellationToken: cancellationToken).ConfigureAwait(false);

        // 2. Declarar a queue do consumer com DLQ (reutiliza logica existente)
        await DeclareQueueWithDlqAsync(channel, queueName, queueOptions, cancellationToken)
            .ConfigureAwait(false);

        // 3. Bind da queue na exchange para cada pattern
        foreach (var pattern in bindingPatterns)
        {
            await channel.QueueBindAsync(
                queue: queueName,
                exchange: exchangeName,
                routingKey: pattern,
                arguments: null,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }
}
