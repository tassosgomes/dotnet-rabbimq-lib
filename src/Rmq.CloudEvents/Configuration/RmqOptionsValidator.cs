using Microsoft.Extensions.Options;

namespace Rmq.CloudEvents.Configuration;

internal sealed class RmqOptionsValidator : IValidateOptions<RmqOptions>
{
    public ValidateOptionsResult Validate(string? name, RmqOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();

        ValidateConnection(options.Connection, failures);
        ValidateCloudEvents(options.DefaultCloudEvents, failures);
        ValidateRetry(options.DefaultRetry, "DefaultRetry", failures);

        if (options.PublishConfirmTimeout <= TimeSpan.Zero)
        {
            failures.Add("PublishConfirmTimeout must be greater than zero.");
        }

        foreach (var (queueName, queueOptions) in options.Queues)
        {
            if (string.IsNullOrWhiteSpace(queueName))
            {
                failures.Add("Queues cannot contain an empty key.");
                continue;
            }

            ValidateQueue(queueOptions, $"Queues['{queueName}']", failures);
        }

        foreach (var (exchangeName, exchangeOptions) in options.Exchanges)
        {
            if (string.IsNullOrWhiteSpace(exchangeName))
            {
                failures.Add("Exchanges cannot contain an empty key.");
                continue;
            }

            ValidateExchange(exchangeName, exchangeOptions, failures);
        }

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }

    private static void ValidateConnection(RmqConnectionOptions options, List<string> failures)
    {
        if (string.IsNullOrWhiteSpace(options.HostName))
        {
            failures.Add("Connection.HostName is required.");
        }

        if (options.Port <= 0)
        {
            failures.Add("Connection.Port must be greater than zero.");
        }

        if (string.IsNullOrWhiteSpace(options.UserName))
        {
            failures.Add("Connection.UserName is required.");
        }

        if (string.IsNullOrWhiteSpace(options.VirtualHost))
        {
            failures.Add("Connection.VirtualHost is required.");
        }

        if (options.NetworkRecoveryInterval <= TimeSpan.Zero)
        {
            failures.Add("Connection.NetworkRecoveryInterval must be greater than zero.");
        }

        if (options.ClientProvidedName is not null && string.IsNullOrWhiteSpace(options.ClientProvidedName))
        {
            failures.Add("Connection.ClientProvidedName cannot be empty when provided.");
        }
    }

    private static void ValidateCloudEvents(CloudEventsOptions options, List<string> failures)
    {
        if (options.Source is null)
        {
            failures.Add("DefaultCloudEvents.Source is required.");
        }

        if (string.IsNullOrWhiteSpace(options.DefaultType))
        {
            failures.Add("DefaultCloudEvents.DefaultType is required.");
        }

        if (!string.Equals(options.SpecVersion, "1.0", StringComparison.Ordinal))
        {
            failures.Add("DefaultCloudEvents.SpecVersion must be '1.0'.");
        }
    }

    private static void ValidateQueue(QueueOptions? options, string path, List<string> failures)
    {
        if (options is null)
        {
            failures.Add($"{path} is required.");
            return;
        }

        if (options.DeliveryLimit <= 0)
        {
            failures.Add($"{path}.DeliveryLimit must be greater than zero.");
        }

        if (options.QuorumSize < 0)
        {
            failures.Add($"{path}.QuorumSize cannot be negative.");
        }

        if (options.Dlq is null)
        {
            failures.Add($"{path}.Dlq is required.");
        }
        else if (options.Dlq.Enabled && string.IsNullOrWhiteSpace(options.Dlq.QueueNameSuffix))
        {
            failures.Add($"{path}.Dlq.QueueNameSuffix is required when DLQ is enabled.");
        }

        ValidateRetry(options.Retry, $"{path}.Retry", failures);
    }

    private static void ValidateExchange(string exchangeName, ExchangeOptions? options, List<string> failures)
    {
        if (options is null)
        {
            failures.Add($"Exchanges['{exchangeName}'] is required.");
            return;
        }

        if (!string.IsNullOrWhiteSpace(options.Name) &&
            !string.Equals(options.Name, exchangeName, StringComparison.Ordinal))
        {
            failures.Add($"Exchanges['{exchangeName}'].Name must match the dictionary key when provided.");
        }
    }

    private static void ValidateRetry(RetryOptions? options, string path, List<string> failures)
    {
        if (options is null)
        {
            failures.Add($"{path} is required.");
            return;
        }

        if (options.MaxAttempts <= 0)
        {
            failures.Add($"{path}.MaxAttempts must be greater than zero.");
        }

        if (options.InitialDelay < TimeSpan.Zero)
        {
            failures.Add($"{path}.InitialDelay cannot be negative.");
        }
    }
}
