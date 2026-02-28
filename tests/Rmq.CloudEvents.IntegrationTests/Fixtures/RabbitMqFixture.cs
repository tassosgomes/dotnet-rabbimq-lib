using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;
using DotNet.Testcontainers.Containers;
using RabbitMQ.Client;
using Testcontainers.RabbitMq;
using Xunit;

namespace Rmq.CloudEvents.IntegrationTests.Fixtures;

public sealed class RabbitMqFixture : IAsyncLifetime
{
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);
    private readonly RabbitMqContainer? _container;
    private HttpClient? _managementClient;
    private Uri? _managementBaseUri;
    private readonly bool _useRemoteBroker;
    private string? _connectionString;

    public RabbitMqFixture()
    {
        var remoteHost = Environment.GetEnvironmentVariable("RABBITMQ_HOST");
        var remoteUser = Environment.GetEnvironmentVariable("RABBITMQ_USER");
        var remotePass = Environment.GetEnvironmentVariable("RABBITMQ_PASS");
        var remoteVhost = Environment.GetEnvironmentVariable("RABBITMQ_VHOST");

        if (!string.IsNullOrWhiteSpace(remoteHost) &&
            !string.IsNullOrWhiteSpace(remoteUser) &&
            !string.IsNullOrWhiteSpace(remotePass))
        {
            _useRemoteBroker = true;
            _managementBaseUri = BuildManagementUri(remoteHost);
            _connectionString = BuildAmqpConnectionString(_managementBaseUri, remoteUser, remotePass, remoteVhost);
            _managementClient = CreateManagementClient(_managementBaseUri, remoteUser, remotePass);
            return;
        }

        _container = new RabbitMqBuilder("rabbitmq:3.13-management").Build();
    }

    public string ConnectionString => _connectionString
        ?? throw new InvalidOperationException("RabbitMQ connection string is not available before fixture initialization.");

    public bool SupportsManagementApi => _managementClient is not null;

    public async Task InitializeAsync()
    {
        await _lifecycleLock.WaitAsync();
        try
        {
            if (_useRemoteBroker)
            {
                await WaitUntilReadyAsync().ConfigureAwait(false);
                return;
            }

            await _container!.StartAsync().ConfigureAwait(false);
            RefreshLocalEndpoints();
            await WaitUntilReadyAsync().ConfigureAwait(false);
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public async Task DisposeAsync()
    {
        await _lifecycleLock.WaitAsync();
        try
        {
            if (_container is not null)
            {
                await _container.DisposeAsync().ConfigureAwait(false);
            }

            _managementClient?.Dispose();
        }
        finally
        {
            _lifecycleLock.Release();
            _lifecycleLock.Dispose();
        }
    }

    public async Task RestartAsync()
    {
        await _lifecycleLock.WaitAsync();
        try
        {
            if (_useRemoteBroker)
            {
                throw new InvalidOperationException("RestartAsync is not supported for remote brokers.");
            }

            await _container!.StopAsync().ConfigureAwait(false);
            await _container.StartAsync().ConfigureAwait(false);
            RefreshLocalEndpoints();
            await WaitUntilReadyAsync().ConfigureAwait(false);
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public async Task InterruptConnectionsByNameAsync(string clientProvidedName)
    {
        if (_managementClient is null)
        {
            throw new InvalidOperationException("Management API is not available for this fixture.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(clientProvidedName);

        var targets = await WaitForConnectionsByNameAsync(clientProvidedName).ConfigureAwait(false);

        if (targets.Count == 0)
        {
            throw new InvalidOperationException($"No active RabbitMQ connections found with client-provided name '{clientProvidedName}'.");
        }

        foreach (var target in targets)
        {
            var encodedName = Uri.EscapeDataString(target.Name);
            using var response = await _managementClient.DeleteAsync($"api/connections/{encodedName}").ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
        }
    }

    private async Task<IReadOnlyList<ManagementConnection>> WaitForConnectionsByNameAsync(string clientProvidedName)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(5);

        while (DateTimeOffset.UtcNow < deadline)
        {
            var connections = await ListConnectionsAsync().ConfigureAwait(false);
            var matches = connections
                .Where(connection => string.Equals(connection.ClientProvidedName, clientProvidedName, StringComparison.Ordinal))
                .ToList();

            if (matches.Count > 0)
            {
                return matches;
            }

            await Task.Delay(200).ConfigureAwait(false);
        }

        return [];
    }

    private async Task WaitUntilReadyAsync()
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        Exception? lastException = null;

        while (DateTimeOffset.UtcNow < deadline)
        {
            try
            {
                var uri = new Uri(ConnectionString);
                var credentials = uri.UserInfo.Split(':', 2);
                var factory = new ConnectionFactory
                {
                    HostName = uri.Host,
                    Port = uri.Port,
                    UserName = credentials.Length > 0 ? Uri.UnescapeDataString(credentials[0]) : "guest",
                    Password = credentials.Length > 1 ? Uri.UnescapeDataString(credentials[1]) : "guest",
                    VirtualHost = NormalizeVirtualHost(uri.AbsolutePath)
                };

                await using var connection = await factory.CreateConnectionAsync();
                await using var channel = await connection.CreateChannelAsync();
                return;
            }
            catch (Exception ex)
            {
                lastException = ex;
                await Task.Delay(500).ConfigureAwait(false);
            }
        }

        throw new TimeoutException("RabbitMQ broker did not become ready in time.", lastException);
    }

    private async Task<IReadOnlyList<ManagementConnection>> ListConnectionsAsync()
    {
        using var response = await _managementClient!.GetAsync("api/connections").ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
        var payload = await JsonSerializer.DeserializeAsync<List<ManagementConnectionDto>>(
            stream,
            new JsonSerializerOptions(JsonSerializerDefaults.Web)).ConfigureAwait(false)
            ?? [];

        return payload
            .Select(item => new ManagementConnection(
                item.Name ?? string.Empty,
                item.UserProvidedName
                ?? item.ClientProperties?.ConnectionName
                ?? string.Empty))
            .ToList();
    }

    private static Uri BuildManagementUri(string rawHost)
    {
        if (Uri.TryCreate(rawHost, UriKind.Absolute, out var absolute))
        {
            return absolute;
        }

        return new Uri($"https://{rawHost.TrimEnd('/')}/");
    }

    private static string BuildAmqpConnectionString(Uri managementUri, string user, string pass, string? vhost)
    {
        var host = managementUri.Host;
        var escapedUser = Uri.EscapeDataString(user);
        var escapedPass = Uri.EscapeDataString(pass);
        var normalizedVhost = string.IsNullOrWhiteSpace(vhost) ? "/" : vhost;
        var escapedVhost = normalizedVhost == "/"
            ? "%2F"
            : Uri.EscapeDataString(normalizedVhost);

        return $"amqp://{escapedUser}:{escapedPass}@{host}:5672/{escapedVhost}";
    }

    private static string NormalizeVirtualHost(string absolutePath)
    {
        var virtualHost = Uri.UnescapeDataString(absolutePath);
        if (string.IsNullOrWhiteSpace(virtualHost) || virtualHost == "/")
        {
            return "/";
        }

        return virtualHost.TrimStart('/');
    }

    private static HttpClient CreateManagementClient(Uri baseUri, string user, string pass)
    {
        var client = new HttpClient
        {
            BaseAddress = baseUri
        };

        var credentials = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{user}:{pass}"));
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        return client;
    }

    private void RefreshLocalEndpoints()
    {
        _connectionString = _container!.GetConnectionString();

        _managementClient?.Dispose();
        _managementClient = null;
        _managementBaseUri = null;

        try
        {
            _managementBaseUri = new Uri($"http://127.0.0.1:{_container.GetMappedPublicPort(15672)}/");
            _managementClient = CreateManagementClient(_managementBaseUri, "guest", "guest");
        }
        catch (InvalidOperationException)
        {
            // Local fixture may expose only AMQP; management API remains optional.
        }
    }

    private sealed record ManagementConnection(string Name, string ClientProvidedName);

    private sealed class ManagementConnectionDto
    {
        [JsonPropertyName("name")]
        public string? Name { get; init; }

        [JsonPropertyName("user_provided_name")]
        public string? UserProvidedName { get; init; }

        [JsonPropertyName("client_properties")]
        public ClientPropertiesDto? ClientProperties { get; init; }
    }

    private sealed class ClientPropertiesDto
    {
        [JsonPropertyName("connection_name")]
        public string? ConnectionName { get; init; }
    }
}
