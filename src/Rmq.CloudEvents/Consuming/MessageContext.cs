namespace Rmq.CloudEvents.Consuming;

/// <summary>
/// Contexto da mensagem recebida com metadados de processamento.
/// </summary>
public sealed class MessageContext
{
    /// <summary>
    /// Identificador do evento (CloudEvent id).
    /// </summary>
    public required string EventId { get; init; }

    /// <summary>
    /// Origem do evento (CloudEvent source).
    /// </summary>
    public required Uri Source { get; init; }

    /// <summary>
    /// Tipo do evento (CloudEvent type).
    /// </summary>
    public required string EventType { get; init; }

    /// <summary>
    /// Timestamp do evento (CloudEvent time).
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Headers customizados da mensagem.
    /// </summary>
    public IReadOnlyDictionary<string, object> Headers { get; init; } = new Dictionary<string, object>();

    /// <summary>
    /// Delivery tag do RabbitMQ.
    /// </summary>
    public ulong DeliveryTag { get; init; }

    /// <summary>
    /// Nome da queue de origem.
    /// </summary>
    public required string QueueName { get; init; }

    /// <summary>
    /// Numero da tentativa atual de processamento.
    /// </summary>
    public int AttemptNumber { get; init; }

    /// <summary>
    /// Nome da exchange de onde a mensagem foi recebida.
    /// Vazio para mensagens da default exchange (publish direto em queue).
    /// </summary>
    public string ExchangeName { get; init; } = string.Empty;

    /// <summary>
    /// Routing key original da mensagem.
    /// Para publish direto em queue, corresponde ao nome da queue.
    /// </summary>
    public string RoutingKey { get; init; } = string.Empty;
}
