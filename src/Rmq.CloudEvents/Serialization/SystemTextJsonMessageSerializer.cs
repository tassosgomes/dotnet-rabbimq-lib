using System.Text.Json;
using System.Text.Json.Serialization;
using Rmq.CloudEvents.Exceptions;

namespace Rmq.CloudEvents.Serialization;

/// <summary>
/// Implementacao de serializacao usando System.Text.Json.
/// </summary>
internal sealed class SystemTextJsonMessageSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <summary>
    /// Inicializa uma nova instancia de <see cref="SystemTextJsonMessageSerializer"/>.
    /// </summary>
    /// <param name="options">Opcoes customizadas de serializacao.</param>
    public SystemTextJsonMessageSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };
    }

    /// <inheritdoc />
    public byte[] Serialize<T>(T value) where T : class
    {
        ArgumentNullException.ThrowIfNull(value);
        return JsonSerializer.SerializeToUtf8Bytes(value, _options);
    }

    /// <inheritdoc />
    public T Deserialize<T>(ReadOnlySpan<byte> data) where T : class
    {
        return JsonSerializer.Deserialize<T>(data, _options)
            ?? throw new RmqConsumeException($"Falha ao deserializar para {typeof(T).Name}");
    }
}
