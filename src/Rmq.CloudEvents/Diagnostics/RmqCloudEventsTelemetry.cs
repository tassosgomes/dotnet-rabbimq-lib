namespace Rmq.CloudEvents.Diagnostics;

/// <summary>
/// Identificadores publicos para integracao com tracing e metricas.
/// </summary>
public static class RmqCloudEventsTelemetry
{
    /// <summary>
    /// Nome do <see cref="System.Diagnostics.ActivitySource"/> usado pela biblioteca.
    /// </summary>
    public const string ActivitySourceName = "Rmq.CloudEvents";

    /// <summary>
    /// Nome do <see cref="System.Diagnostics.Metrics.Meter"/> usado pela biblioteca.
    /// </summary>
    public const string MeterName = "Rmq.CloudEvents";

    /// <summary>
    /// Versao exposta para instrumentacao.
    /// </summary>
    public const string Version = "1.0.0";
}
