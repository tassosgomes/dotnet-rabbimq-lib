using RabbitMQ.Client;

namespace Rmq.CloudEvents.Configuration;

/// <summary>
/// Configuracoes de conexao com RabbitMQ.
/// </summary>
public sealed class RmqConnectionOptions
{
    /// <summary>
    /// Nome do host RabbitMQ.
    /// </summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>
    /// Porta RabbitMQ.
    /// </summary>
    public int Port { get; set; } = 5672;

    /// <summary>
    /// Usuario para autenticacao.
    /// </summary>
    public string UserName { get; set; } = "guest";

    /// <summary>
    /// Senha para autenticacao.
    /// </summary>
    public string Password { get; set; } = "guest";

    /// <summary>
    /// Virtual host RabbitMQ.
    /// </summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// Configuracao SSL/TLS opcional.
    /// </summary>
    public SslOption? Ssl { get; set; }

    /// <summary>
    /// Intervalo de recuperacao de rede.
    /// </summary>
    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(10);
}
