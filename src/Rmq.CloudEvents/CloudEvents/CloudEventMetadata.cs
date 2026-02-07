namespace Rmq.CloudEvents.CloudEvents;

/// <summary>
/// Metadados relevantes extraidos de um CloudEvent.
/// </summary>
/// <param name="EventId">Identificador do evento.</param>
/// <param name="Source">Origem do evento.</param>
/// <param name="EventType">Tipo do evento.</param>
/// <param name="Timestamp">Timestamp do evento.</param>
internal sealed record CloudEventMetadata(
    string EventId,
    Uri Source,
    string EventType,
    DateTimeOffset Timestamp);
