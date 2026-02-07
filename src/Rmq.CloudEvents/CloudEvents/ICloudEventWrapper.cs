namespace Rmq.CloudEvents.CloudEvents;

/// <summary>
/// Encapsula e desencapsula payloads em CloudEvents.
/// </summary>
internal interface ICloudEventWrapper
{
    /// <summary>
    /// Encapsula um payload em um CloudEvent no modo structured JSON.
    /// </summary>
    /// <typeparam name="T">Tipo do payload.</typeparam>
    /// <param name="payload">Payload para encapsular.</param>
    /// <param name="eventType">Tipo do evento opcional.</param>
    /// <returns>CloudEvent serializado em UTF-8.</returns>
    ReadOnlyMemory<byte> Wrap<T>(T payload, string? eventType = null) where T : class;

    /// <summary>
    /// Desencapsula um CloudEvent e retorna payload com metadados.
    /// </summary>
    /// <typeparam name="T">Tipo do payload esperado.</typeparam>
    /// <param name="data">Dados de entrada no formato CloudEvent structured JSON.</param>
    /// <returns>Payload tipado e metadados do evento.</returns>
    (T Payload, CloudEventMetadata Metadata) Unwrap<T>(ReadOnlyMemory<byte> data) where T : class;
}
