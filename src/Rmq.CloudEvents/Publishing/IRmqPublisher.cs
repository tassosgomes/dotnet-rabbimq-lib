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
    /// <summary>
    /// Publica um payload em uma Topic Exchange com routing key.
    /// </summary>
    /// <typeparam name="T">Tipo do payload.</typeparam>
    /// <param name="exchangeName">Nome da exchange topic.</param>
    /// <param name="routingKey">Routing key para o publish.</param>
    /// <param name="payload">Payload a publicar.</param>
    /// <param name="cloudEventType">Tipo do CloudEvent opcional.</param>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    Task PublishToTopicAsync<T>(
        string exchangeName,
        string routingKey,
        T payload,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    /// Publica um payload em uma Topic Exchange com routing key e headers customizados.
    /// </summary>
    /// <typeparam name="T">Tipo do payload.</typeparam>
    /// <param name="exchangeName">Nome da exchange topic.</param>
    /// <param name="routingKey">Routing key para o publish.</param>
    /// <param name="payload">Payload a publicar.</param>
    /// <param name="headers">Headers customizados da mensagem.</param>
    /// <param name="cloudEventType">Tipo do CloudEvent opcional.</param>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    Task PublishToTopicAsync<T>(
        string exchangeName,
        string routingKey,
        T payload,
        IDictionary<string, object> headers,
        string? cloudEventType = null,
        CancellationToken cancellationToken = default)
        where T : class;
}
