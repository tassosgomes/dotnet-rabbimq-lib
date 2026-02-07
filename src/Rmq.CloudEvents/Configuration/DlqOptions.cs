namespace Rmq.CloudEvents.Configuration;

/// <summary>
/// Configuracoes de dead-letter queue (DLQ).
/// </summary>
public sealed class DlqOptions
{
    /// <summary>
    /// Habilita a configuracao automatica de DLQ.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Sufixo utilizado no nome da DLQ.
    /// </summary>
    public string QueueNameSuffix { get; set; } = ".dlq";
}
