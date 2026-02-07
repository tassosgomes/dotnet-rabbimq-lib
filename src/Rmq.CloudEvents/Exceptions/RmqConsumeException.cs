namespace Rmq.CloudEvents.Exceptions;

/// <summary>
/// Excecao para falhas de consumo/processamento de mensagens.
/// </summary>
public sealed class RmqConsumeException : RmqCloudEventsException
{
    /// <summary>
    /// Inicializa uma nova instancia com mensagem.
    /// </summary>
    /// <param name="message">Mensagem de erro.</param>
    public RmqConsumeException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Inicializa uma nova instancia com mensagem e inner exception.
    /// </summary>
    /// <param name="message">Mensagem de erro.</param>
    /// <param name="innerException">Excecao interna.</param>
    public RmqConsumeException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
