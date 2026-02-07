namespace Rmq.CloudEvents.Consuming;

/// <summary>
/// Gerencia o ciclo de vida do consumo de mensagens.
/// </summary>
public interface IRmqConsumer : IAsyncDisposable
{
    /// <summary>
    /// Inicia o consumo de mensagens.
    /// </summary>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Para o consumo de mensagens.
    /// </summary>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    Task StopAsync(CancellationToken cancellationToken = default);
}
