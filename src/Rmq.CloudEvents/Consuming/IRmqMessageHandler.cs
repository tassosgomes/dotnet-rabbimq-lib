namespace Rmq.CloudEvents.Consuming;

/// <summary>
/// Handler de mensagens implementado pelo desenvolvedor.
/// </summary>
/// <typeparam name="T">Tipo do payload recebido.</typeparam>
public interface IRmqMessageHandler<in T>
    where T : class
{
    /// <summary>
    /// Processa uma mensagem recebida.
    /// </summary>
    /// <param name="message">Payload deserializado.</param>
    /// <param name="context">Metadados da mensagem.</param>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    Task HandleAsync(T message, MessageContext context, CancellationToken cancellationToken);
}
