using RabbitMQ.Client;

namespace Rmq.CloudEvents.Connection;

/// <summary>
/// Gerencia conexoes e canais com RabbitMQ.
/// </summary>
internal interface IRmqConnectionManager : IAsyncDisposable
{
    /// <summary>
    /// Obtem uma conexao aberta, criando uma nova se necessario.
    /// </summary>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    /// <returns>Conexao ativa.</returns>
    Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Cria um novo canal a partir da conexao ativa.
    /// </summary>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    /// <returns>Canal AMQP.</returns>
    Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Cria um canal configurado para publish com confirmacoes do broker.
    /// </summary>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    /// <returns>Canal AMQP com publisher confirms habilitado.</returns>
    Task<IChannel> CreatePublisherChannelAsync(CancellationToken cancellationToken = default);
}
