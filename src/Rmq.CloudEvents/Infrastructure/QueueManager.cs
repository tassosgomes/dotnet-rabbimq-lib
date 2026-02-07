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
}
