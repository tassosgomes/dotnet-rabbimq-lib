namespace Rmq.CloudEvents.Configuration;

/// <summary>
/// Configuracoes raiz da biblioteca.
/// </summary>
public sealed class RmqOptions
{
    /// <summary>
    /// Configuracoes de conexao com RabbitMQ.
    /// </summary>
    public RmqConnectionOptions Connection { get; set; } = new();

    /// <summary>
    /// Configuracoes padrao de CloudEvents.
    /// </summary>
    public CloudEventsOptions DefaultCloudEvents { get; set; } = new();

    /// <summary>
    /// Configuracoes de retry padrao.
    /// </summary>
    public RetryOptions DefaultRetry { get; set; } = new();

    /// <summary>
    /// Configuracoes especificas por queue.
    /// </summary>
    public Dictionary<string, QueueOptions> Queues { get; set; } = new();
}
