namespace Rmq.CloudEvents.Serialization;

/// <summary>
/// Contrato de serializacao de mensagens.
/// </summary>
internal interface IMessageSerializer
{
    /// <summary>
    /// Serializa um valor para bytes UTF-8.
    /// </summary>
    /// <typeparam name="T">Tipo do valor.</typeparam>
    /// <param name="value">Valor de entrada.</param>
    /// <returns>Bytes serializados.</returns>
    byte[] Serialize<T>(T value) where T : class;

    /// <summary>
    /// Deserializa bytes UTF-8 para o tipo informado.
    /// </summary>
    /// <typeparam name="T">Tipo de destino.</typeparam>
    /// <param name="data">Dados serializados.</param>
    /// <returns>Instancia deserializada.</returns>
    T Deserialize<T>(ReadOnlySpan<byte> data) where T : class;
}
