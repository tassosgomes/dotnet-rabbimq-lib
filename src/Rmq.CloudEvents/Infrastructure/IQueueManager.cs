using RabbitMQ.Client;
using Rmq.CloudEvents.Configuration;

namespace Rmq.CloudEvents.Infrastructure;

/// <summary>
/// Gerencia declaracao de queues e DLQs.
/// </summary>
internal interface IQueueManager
{
    /// <summary>
    /// Declara queue quorum com DLQ, DLX e bind associados.
    /// </summary>
    /// <param name="channel">Canal de operacao.</param>
    /// <param name="queueName">Nome da queue principal.</param>
    /// <param name="options">Configuracoes da queue.</param>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    Task DeclareQueueWithDlqAsync(
        IChannel channel,
        string queueName,
        QueueOptions options,
        CancellationToken cancellationToken = default);
}
