namespace Rmq.CloudEvents.Configuration;

/// <summary>
/// Configuracoes padrao de CloudEvents.
/// </summary>
public sealed class CloudEventsOptions
{
    /// <summary>
    /// URI de origem dos eventos (CloudEvent source).
    /// </summary>
    public Uri Source { get; set; } = new("/undefined", UriKind.Relative);

    /// <summary>
    /// Tipo padrao dos eventos (CloudEvent type).
    /// </summary>
    public string DefaultType { get; set; } = "com.default.event.v1";

    /// <summary>
    /// Versao do spec CloudEvents.
    /// </summary>
    public string SpecVersion { get; set; } = "1.0";
}
