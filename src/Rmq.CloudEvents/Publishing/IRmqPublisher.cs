namespace Rmq.CloudEvents.Publishing;

/// <summary>
/// Publica mensagens em queues RabbitMQ com CloudEvents transparente.
/// </summary>
public interface IRmqPublisher : IAsyncDisposable
{
    /// <summary>
    /// Publica um payload na queue especificada.
    /// </summary>
    /// <typeparam name="T">Tipo do payload.</typeparam>
    /// <param name="queueName">Nome da queue destino.</param>
    /// <param name="payload">Payload a publicar.</param>
    /// <param name="cloudEventType">Tipo do CloudEvent opcional.</param>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    Task PublishAsync<T>(
        string queueName,
        T payload,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    /// Publica um payload na queue especificada com headers customizados.
    /// </summary>
    /// <typeparam name="T">Tipo do payload.</typeparam>
    /// <param name="queueName">Nome da queue destino.</param>
    /// <param name="payload">Payload a publicar.</param>
    /// <param name="headers">Headers customizados da mensagem.</param>
    /// <param name="cloudEventType">Tipo do CloudEvent opcional.</param>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    Task PublishAsync<T>(
        string queueName,
        T payload,
        IDictionary<string, object> headers,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class;
}
