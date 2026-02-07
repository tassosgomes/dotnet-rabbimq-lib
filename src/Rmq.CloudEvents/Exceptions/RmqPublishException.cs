namespace Rmq.CloudEvents.Exceptions;

/// <summary>
/// Excecao para falhas de publish apos esgotar retries.
/// </summary>
public sealed class RmqPublishException : RmqCloudEventsException
{
    /// <summary>
    /// Inicializa uma nova instancia de <see cref="RmqPublishException"/>.
    /// </summary>
    /// <param name="queueName">Nome da queue de destino.</param>
    /// <param name="attemptsExhausted">Quantidade de tentativas esgotadas.</param>
    /// <param name="innerException">Excecao interna original.</param>
    public RmqPublishException(string queueName, int attemptsExhausted, Exception innerException)
        : base($"Falha ao publicar na queue '{queueName}' apos {attemptsExhausted} tentativas.", innerException)
    {
        QueueName = queueName;
        AttemptsExhausted = attemptsExhausted;
    }

    /// <summary>
    /// Nome da queue onde o publish falhou.
    /// </summary>
    public string QueueName { get; }

    /// <summary>
    /// Quantidade de tentativas esgotadas.
    /// </summary>
    public int AttemptsExhausted { get; }
}
