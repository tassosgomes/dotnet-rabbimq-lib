namespace Rmq.CloudEvents.Configuration;

/// <summary>
/// Configuracoes de uma exchange RabbitMQ.
/// </summary>
public sealed class ExchangeOptions
{
    /// <summary>
    /// Nome da exchange.
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Se a exchange eh duravel (sobrevive restart do broker).
    /// </summary>
    public bool Durable { get; set; } = true;

    /// <summary>
    /// Se a exchange eh auto-deletada quando nao ha mais bindings.
    /// </summary>
    public bool AutoDelete { get; set; } = false;

    /// <summary>
    /// Argumentos customizados para declaracao da exchange.
    /// </summary>
    public IDictionary<string, object>? Arguments { get; set; }
}