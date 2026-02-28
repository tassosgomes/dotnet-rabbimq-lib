using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Rmq.CloudEvents.Configuration;

namespace Rmq.CloudEvents.Connection;

/// <summary>
/// Gerencia o ciclo de vida da conexao com RabbitMQ.
/// </summary>
internal sealed class RmqConnectionManager : IRmqConnectionManager
{
    private readonly RmqConnectionOptions _options;
    private readonly Func<ConnectionFactory> _connectionFactoryFactory;
    private readonly Func<ConnectionFactory, CancellationToken, Task<IConnection>> _createConnection;
    private readonly ILogger<RmqConnectionManager> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private IConnection? _connection;

    /// <summary>
    /// Inicializa uma nova instancia de <see cref="RmqConnectionManager"/>.
    /// </summary>
    /// <param name="options">Configuracao de conexao.</param>
    /// <param name="logger">Logger opcional.</param>
    public RmqConnectionManager(
        RmqConnectionOptions options,
        ILogger<RmqConnectionManager>? logger = null)
        : this(options, null, null, logger)
    {
    }

    internal RmqConnectionManager(
        RmqConnectionOptions options,
        Func<ConnectionFactory>? connectionFactoryFactory,
        Func<ConnectionFactory, CancellationToken, Task<IConnection>>? createConnection,
        ILogger<RmqConnectionManager>? logger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _connectionFactoryFactory = connectionFactoryFactory ?? CreateConnectionFactory;
        _createConnection = createConnection ?? ((factory, ct) => factory.CreateConnectionAsync(ct));
        _logger = logger ?? NullLogger<RmqConnectionManager>.Instance;
    }

    /// <inheritdoc />
    public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        if (_connection is { IsOpen: true })
        {
            return _connection;
        }

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connection is { IsOpen: true })
            {
                return _connection;
            }

            if (_connection is not null)
            {
                await _connection.DisposeAsync().ConfigureAwait(false);
                _connection = null;
            }

            var factory = _connectionFactoryFactory();
            _connection = await _createConnection(factory, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Conexao RabbitMQ estabelecida em {Host}:{Port}", _options.HostName, _options.Port);
            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    /// <inheritdoc />
    public async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IChannel> CreatePublisherChannelAsync(CancellationToken cancellationToken = default)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await connection.CreateChannelAsync(
            new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true,
                outstandingPublisherConfirmationsRateLimiter: null),
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
            _connection = null;
        }

        _connectionLock.Dispose();
    }

    private ConnectionFactory CreateConnectionFactory()
    {
        var factory = new ConnectionFactory
        {
            HostName = _options.HostName,
            Port = _options.Port,
            UserName = _options.UserName,
            Password = _options.Password,
            VirtualHost = _options.VirtualHost,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            NetworkRecoveryInterval = _options.NetworkRecoveryInterval
        };

        if (_options.Ssl is not null)
        {
            factory.Ssl = _options.Ssl;
        }

        return factory;
    }
}
