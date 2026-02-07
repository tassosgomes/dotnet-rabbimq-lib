using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Consuming;
using Rmq.CloudEvents.Extensions;
using Rmq.CloudEvents.Publishing;

namespace Rmq.CloudEvents.Sample;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        // 1) Build de host e registro da biblioteca no container DI.
        var builder = Host.CreateApplicationBuilder(args);
        builder.Logging.AddSimpleConsole(options => options.TimestampFormat = "HH:mm:ss ");

        builder.Services.AddRmqCloudEvents(options =>
        {
            options.Connection = new RmqConnectionOptions
            {
                HostName = "localhost",
                Port = 5672,
                UserName = "guest",
                Password = "guest",
                VirtualHost = "/"
            };

            options.DefaultCloudEvents = new CloudEventsOptions
            {
                Source = new Uri("/rmq-cloudevents-sample", UriKind.Relative),
                DefaultType = "com.sample.order.created"
            };
        });

        // 2) Registro do consumer para a queue "orders".
        builder.Services.AddRmqConsumer<OrderCreated, OrderCreatedConsumer>("orders");

        using var host = builder.Build();
        await host.StartAsync();

        // 3) Publicacao de uma mensagem de exemplo para demonstrar o fluxo completo.
        var publisher = host.Services.GetRequiredService<IRmqPublisher>();
        var order = new OrderCreated(
            OrderId: 1,
            CustomerId: "cust-001",
            Total: 99.90m,
            Items: ["notebook", "mouse"]);

        await publisher.PublishAsync("orders", order);
        Console.WriteLine("Order publicada. Aguarde o consumer processar...");

        await Task.Delay(TimeSpan.FromSeconds(3));
        await host.StopAsync();
    }

    private sealed record OrderCreated(int OrderId, string CustomerId, decimal Total, IReadOnlyList<string> Items);

    // 4) Handler do consumidor: recebe o payload puro (CloudEvent transparente).
    private sealed class OrderCreatedConsumer : IRmqMessageHandler<OrderCreated>
    {
        private readonly ILogger<OrderCreatedConsumer> _logger;

        public OrderCreatedConsumer(ILogger<OrderCreatedConsumer> logger)
        {
            _logger = logger;
        }

        public Task HandleAsync(OrderCreated message, MessageContext context, CancellationToken cancellationToken)
        {
            _logger.LogInformation(
                "Order {OrderId} recebida para customer {CustomerId}. EventId={EventId}, Queue={QueueName}",
                message.OrderId,
                message.CustomerId,
                context.EventId,
                context.QueueName);

            return Task.CompletedTask;
        }
    }
}
