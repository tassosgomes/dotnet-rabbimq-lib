namespace Rmq.CloudEvents.Configuration;

/// <summary>
/// Configuracoes especificas por queue.
/// </summary>
public sealed class QueueOptions
{
    /// <summary>
    /// Quantidade maxima de mensagens entregues sem ACK por consumer.
    /// Zero delega o comportamento ao broker.
    /// </summary>
    public ushort PrefetchCount { get; set; } = 0;

    /// <summary>
    /// Tamanho inicial do grupo quorum (0 usa default do RabbitMQ).
    /// </summary>
    public int QuorumSize { get; set; } = 0;

    /// <summary>
    /// Limite de entregas antes de dead-letter.
    /// </summary>
    public int DeliveryLimit { get; set; } = 5;

    /// <summary>
    /// Configuracoes de retry da queue.
    /// </summary>
    public RetryOptions Retry { get; set; } = new();

    /// <summary>
    /// Configuracoes de DLQ da queue.
    /// </summary>
    public DlqOptions Dlq { get; set; } = new();
}
