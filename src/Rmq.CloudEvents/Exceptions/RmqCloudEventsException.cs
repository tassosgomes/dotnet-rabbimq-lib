namespace Rmq.CloudEvents.Exceptions;

/// <summary>
/// Excecao base da biblioteca Rmq.CloudEvents.
/// </summary>
public class RmqCloudEventsException : Exception
{
    /// <summary>
    /// Inicializa uma nova instancia com mensagem.
    /// </summary>
    /// <param name="message">Mensagem de erro.</param>
    public RmqCloudEventsException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Inicializa uma nova instancia com mensagem e inner exception.
    /// </summary>
    /// <param name="message">Mensagem de erro.</param>
    /// <param name="innerException">Excecao interna.</param>
    public RmqCloudEventsException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
