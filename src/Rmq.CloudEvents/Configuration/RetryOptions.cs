namespace Rmq.CloudEvents.Configuration;

/// <summary>
/// Tipos de estrategia de backoff para retry.
/// </summary>
public enum BackoffType
{
    /// <summary>
    /// Backoff exponencial.
    /// </summary>
    Exponential,

    /// <summary>
    /// Backoff linear.
    /// </summary>
    Linear,

    /// <summary>
    /// Delay constante entre tentativas.
    /// </summary>
    Constant
}

/// <summary>
/// Configuracoes de retry para publicacao e consumo.
/// </summary>
public sealed class RetryOptions
{
    /// <summary>
    /// Numero maximo de tentativas.
    /// </summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>
    /// Delay inicial entre retries.
    /// </summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Tipo de backoff utilizado.
    /// </summary>
    public BackoffType BackoffType { get; set; } = BackoffType.Exponential;

    /// <summary>
    /// Indica se deve usar jitter no delay entre tentativas.
    /// </summary>
    public bool UseJitter { get; set; } = true;
}
