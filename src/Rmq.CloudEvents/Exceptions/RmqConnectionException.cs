namespace Rmq.CloudEvents.Exceptions;

/// <summary>
/// Excecao para falhas de conexao com RabbitMQ.
/// </summary>
public sealed class RmqConnectionException : RmqCloudEventsException
{
    /// <summary>
    /// Inicializa uma nova instancia com mensagem.
    /// </summary>
    /// <param name="message">Mensagem de erro.</param>
    public RmqConnectionException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Inicializa uma nova instancia com mensagem e inner exception.
    /// </summary>
    /// <param name="message">Mensagem de erro.</param>
    /// <param name="innerException">Excecao interna.</param>
    public RmqConnectionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
