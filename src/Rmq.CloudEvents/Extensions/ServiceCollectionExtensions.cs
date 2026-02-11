using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Rmq.CloudEvents.CloudEvents;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Connection;
using Rmq.CloudEvents.Consuming;
using Rmq.CloudEvents.Infrastructure;
using Rmq.CloudEvents.Publishing;
using Rmq.CloudEvents.Serialization;

namespace Rmq.CloudEvents.Extensions;

/// <summary>
/// Metodos de extensao para registro dos servicos da biblioteca em DI.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registra os servicos principais da biblioteca Rmq.CloudEvents.
    /// </summary>
    /// <param name="services">Colecao de servicos.</param>
    /// <param name="configure">Delegate de configuracao das opcoes.</param>
    /// <returns>A propria colecao para encadeamento.</returns>
    public static IServiceCollection AddRmqCloudEvents(
        this IServiceCollection services,
        Action<RmqOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<RmqOptions>>().Value);
        services.AddSingleton(sp => sp.GetRequiredService<RmqOptions>().Connection);
        services.AddSingleton(sp => sp.GetRequiredService<RmqOptions>().DefaultCloudEvents);

        services.AddSingleton<IRmqConnectionManager, RmqConnectionManager>();
        services.AddSingleton<IQueueManager, QueueManager>();
        services.AddSingleton<ICloudEventWrapper, CloudEventWrapper>();
        services.AddSingleton<IMessageSerializer, SystemTextJsonMessageSerializer>();
        services.AddTransient<IRmqPublisher, RmqPublisher>();

        return services;
    }

    /// <summary>
    /// Registra um consumer para uma queue especifica.
    /// </summary>
    /// <typeparam name="TMessage">Tipo da mensagem.</typeparam>
    /// <typeparam name="THandler">Tipo do handler.</typeparam>
    /// <param name="services">Colecao de servicos.</param>
    /// <param name="queueName">Nome da queue.</param>
    /// <returns>A propria colecao para encadeamento.</returns>
    public static IServiceCollection AddRmqConsumer<TMessage, THandler>(
        this IServiceCollection services,
        string queueName)
        where TMessage : class
        where THandler : class, IRmqMessageHandler<TMessage>
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        services.AddTransient<IRmqMessageHandler<TMessage>, THandler>();
        services.AddHostedService(sp =>
            new RmqConsumer<TMessage>(
                sp.GetRequiredService<IRmqConnectionManager>(),
                sp.GetRequiredService<IQueueManager>(),
                sp.GetRequiredService<ICloudEventWrapper>(),
                sp.GetRequiredService<IRmqMessageHandler<TMessage>>(),
                sp.GetRequiredService<RmqOptions>(),
                queueName,
                sp.GetService<Microsoft.Extensions.Logging.ILogger<RmqConsumer<TMessage>>>()));

        return services;
    }
}
