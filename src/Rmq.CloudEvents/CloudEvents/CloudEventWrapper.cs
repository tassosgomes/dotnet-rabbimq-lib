using System.Text.Json;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using Rmq.CloudEvents.Configuration;
using Rmq.CloudEvents.Exceptions;

namespace Rmq.CloudEvents.CloudEvents;

/// <summary>
/// Implementacao de encapsulamento/desencapsulamento de CloudEvents.
/// </summary>
internal sealed class CloudEventWrapper : ICloudEventWrapper
{
    private readonly CloudEventsOptions _options;
    private readonly JsonEventFormatter _formatter;
    private readonly JsonSerializerOptions _jsonOptions;

    /// <summary>
    /// Inicializa uma nova instancia de <see cref="CloudEventWrapper"/>.
    /// </summary>
    /// <param name="options">Opcoes padrao de CloudEvents.</param>
    /// <param name="jsonOptions">Opcoes de serializacao JSON.</param>
    public CloudEventWrapper(CloudEventsOptions options, JsonSerializerOptions? jsonOptions = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _formatter = new JsonEventFormatter();
        _jsonOptions = jsonOptions ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);
    }

    /// <inheritdoc />
    public ReadOnlyMemory<byte> Wrap<T>(T payload, string? eventType = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(payload);

        var cloudEvent = new CloudNative.CloudEvents.CloudEvent(CloudEventsSpecVersion.V1_0)
        {
            Id = Guid.NewGuid().ToString(),
            Source = _options.Source,
            Type = eventType ?? _options.DefaultType,
            Time = DateTimeOffset.UtcNow,
            DataContentType = "application/json",
            Data = payload
        };

        return _formatter.EncodeStructuredModeMessage(cloudEvent, out _);
    }

    /// <inheritdoc />
    public (T Payload, CloudEventMetadata Metadata) Unwrap<T>(ReadOnlyMemory<byte> data) where T : class
    {
        CloudNative.CloudEvents.CloudEvent cloudEvent;
        try
        {
            cloudEvent = _formatter.DecodeStructuredModeMessage(data, null, null);
        }
        catch (Exception exception)
        {
            throw new RmqConsumeException("Falha ao decodificar CloudEvent em modo structured JSON.", exception);
        }

        var payload = cloudEvent.Data switch
        {
            T typed => typed,
            JsonElement jsonElement => jsonElement.Deserialize<T>(_jsonOptions)
                ?? throw new RmqConsumeException($"Falha ao deserializar payload do tipo {typeof(T).Name}"),
            _ => throw new RmqConsumeException($"Tipo de data inesperado: {cloudEvent.Data?.GetType().Name ?? "null"}")
        };

        var metadata = new CloudEventMetadata(
            EventId: cloudEvent.Id ?? string.Empty,
            Source: cloudEvent.Source ?? _options.Source,
            EventType: cloudEvent.Type ?? _options.DefaultType,
            Timestamp: cloudEvent.Time ?? DateTimeOffset.UtcNow);

        return (payload, metadata);
    }
}
