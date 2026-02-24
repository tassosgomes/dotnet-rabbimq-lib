namespace Rmq.CloudEvents.Configuration;

/// <summary>
/// Configuracoes para inscricao de um consumer em uma Topic Exchange.
/// </summary>
public sealed class TopicSubscriptionOptions
{
    /// <summary>
    /// Nome da exchange topic a qual se inscrever.
    /// </summary>
    public required string ExchangeName { get; set; }

    /// <summary>
    /// Nome da queue que sera criada/usada para este consumer.
    /// Recomendado: usar nomes fixos para durabilidade.
    /// </summary>
    public string? QueueName { get; set; }

    /// <summary>
    /// Routing key patterns para binding na exchange.
    /// Deve conter pelo menos um pattern.
    /// </summary>
    public required IReadOnlyList<string> BindingPatterns { get; set; }

    /// <summary>
    /// Configuracoes da queue (quorum size, delivery limit, retry, DLQ).
    /// </summary>
    public QueueOptions Queue { get; set; } = new();
}
