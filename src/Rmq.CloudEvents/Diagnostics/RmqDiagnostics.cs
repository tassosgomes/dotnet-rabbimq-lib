using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Rmq.CloudEvents.Diagnostics;

internal static class RmqDiagnostics
{
    public static readonly ActivitySource ActivitySource = new(RmqCloudEventsTelemetry.ActivitySourceName);
    private static readonly Meter Meter = new(RmqCloudEventsTelemetry.MeterName, RmqCloudEventsTelemetry.Version);

    private static readonly Counter<long> PublishAttempts = Meter.CreateCounter<long>("rmq.publish.attempts");
    private static readonly Counter<long> PublishSuccesses = Meter.CreateCounter<long>("rmq.publish.successes");
    private static readonly Counter<long> PublishFailures = Meter.CreateCounter<long>("rmq.publish.failures");
    private static readonly Counter<long> PublishRetries = Meter.CreateCounter<long>("rmq.publish.retries");
    private static readonly Histogram<double> PublishDurationMs = Meter.CreateHistogram<double>("rmq.publish.duration.ms");

    private static readonly Counter<long> ConsumeAttempts = Meter.CreateCounter<long>("rmq.consume.attempts");
    private static readonly Counter<long> ConsumeSuccesses = Meter.CreateCounter<long>("rmq.consume.successes");
    private static readonly Counter<long> ConsumeFailures = Meter.CreateCounter<long>("rmq.consume.failures");
    private static readonly Counter<long> ConsumeRetries = Meter.CreateCounter<long>("rmq.consume.retries");
    private static readonly Histogram<double> ConsumeDurationMs = Meter.CreateHistogram<double>("rmq.consume.duration.ms");

    private static readonly Counter<long> ConnectionAttempts = Meter.CreateCounter<long>("rmq.connection.attempts");
    private static readonly Counter<long> ConnectionSuccesses = Meter.CreateCounter<long>("rmq.connection.successes");
    private static readonly Counter<long> ConnectionFailures = Meter.CreateCounter<long>("rmq.connection.failures");
    private static readonly Histogram<double> ConnectionDurationMs = Meter.CreateHistogram<double>("rmq.connection.duration.ms");

    public static Activity? StartPublishActivity(string destinationKind, string destinationName, string routingKey)
    {
        var activity = ActivitySource.StartActivity("rmq.publish", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.operation.name", "publish");
        activity?.SetTag("messaging.destination.kind", destinationKind);
        activity?.SetTag("messaging.destination.name", destinationName);
        activity?.SetTag("messaging.rabbitmq.destination.routing_key", routingKey);
        return activity;
    }

    public static Activity? StartConsumeActivity(string queueName, string exchangeName, string routingKey, string eventType, string eventId)
    {
        var activity = ActivitySource.StartActivity("rmq.consume", ActivityKind.Consumer);
        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.operation.name", "process");
        activity?.SetTag("messaging.destination.name", queueName);
        activity?.SetTag("messaging.rabbitmq.exchange", exchangeName);
        activity?.SetTag("messaging.rabbitmq.destination.routing_key", routingKey);
        activity?.SetTag("messaging.message.id", eventId);
        activity?.SetTag("messaging.message.type", eventType);
        return activity;
    }

    public static Activity? StartConnectionActivity(string hostName, int port)
    {
        var activity = ActivitySource.StartActivity("rmq.connection.open", ActivityKind.Client);
        activity?.SetTag("server.address", hostName);
        activity?.SetTag("server.port", port);
        activity?.SetTag("messaging.system", "rabbitmq");
        return activity;
    }

    public static void RecordPublishAttempt(string destinationKind, string destinationName) =>
        PublishAttempts.Add(1, CreateTags(destinationKind, destinationName));

    public static void RecordPublishSuccess(string destinationKind, string destinationName, double elapsedMs)
    {
        var tags = CreateTags(destinationKind, destinationName);
        PublishSuccesses.Add(1, tags);
        PublishDurationMs.Record(elapsedMs, tags);
    }

    public static void RecordPublishFailure(string destinationKind, string destinationName, double elapsedMs)
    {
        var tags = CreateTags(destinationKind, destinationName);
        PublishFailures.Add(1, tags);
        PublishDurationMs.Record(elapsedMs, tags);
    }

    public static void RecordPublishRetry(string destinationKind, string destinationName) =>
        PublishRetries.Add(1, CreateTags(destinationKind, destinationName));

    public static void RecordConsumeAttempt(string queueName) =>
        ConsumeAttempts.Add(1, new TagList { { "messaging.destination.name", queueName } });

    public static void RecordConsumeSuccess(string queueName, double elapsedMs)
    {
        var tags = new TagList { { "messaging.destination.name", queueName } };
        ConsumeSuccesses.Add(1, tags);
        ConsumeDurationMs.Record(elapsedMs, tags);
    }

    public static void RecordConsumeFailure(string queueName, double elapsedMs)
    {
        var tags = new TagList { { "messaging.destination.name", queueName } };
        ConsumeFailures.Add(1, tags);
        ConsumeDurationMs.Record(elapsedMs, tags);
    }

    public static void RecordConsumeRetry(string queueName) =>
        ConsumeRetries.Add(1, new TagList { { "messaging.destination.name", queueName } });

    public static void RecordConnectionAttempt(string hostName, int port) =>
        ConnectionAttempts.Add(1, CreateConnectionTags(hostName, port));

    public static void RecordConnectionSuccess(string hostName, int port, double elapsedMs)
    {
        var tags = CreateConnectionTags(hostName, port);
        ConnectionSuccesses.Add(1, tags);
        ConnectionDurationMs.Record(elapsedMs, tags);
    }

    public static void RecordConnectionFailure(string hostName, int port, double elapsedMs)
    {
        var tags = CreateConnectionTags(hostName, port);
        ConnectionFailures.Add(1, tags);
        ConnectionDurationMs.Record(elapsedMs, tags);
    }

    private static TagList CreateTags(string destinationKind, string destinationName) =>
        new()
        {
            { "messaging.destination.kind", destinationKind },
            { "messaging.destination.name", destinationName }
        };

    private static TagList CreateConnectionTags(string hostName, int port) =>
        new()
        {
            { "server.address", hostName },
            { "server.port", port }
        };
}
