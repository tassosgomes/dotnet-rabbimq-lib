---
status: pending
parallelizable: true
blocked_by: [1.0, 2.0]
---

<task_context>
<domain>engine/messaging</domain>
<type>implementation</type>
<scope>core_feature</scope>
<complexity>high</complexity>
<dependencies>task_1, task_2</dependencies>
<unblocks>"7.0"</unblocks>
</task_context>

# Tarefa 3.0: Publisher — PublishToTopicAsync

## Visao Geral

Estender `IRmqPublisher` e `RmqPublisher` com os novos metodos `PublishToTopicAsync` para publicar mensagens em uma Topic Exchange com routing key. Inclui logica de cache de exchanges declaradas, retry com Polly e encapsulamento CloudEvent. A API existente (`PublishAsync` direto em queue) permanece inalterada.

<requirements>
- Adicionar novos metodos na interface `Publishing/IRmqPublisher.cs`:
  - `PublishToTopicAsync<T>(exchangeName, routingKey, payload, cloudEventType?, cancellationToken)`
  - `PublishToTopicAsync<T>(exchangeName, routingKey, payload, headers, cloudEventType?, cancellationToken)`
- Implementar em `Publishing/RmqPublisher.cs`:
  - Metodo privado `PublishToTopicInternalAsync` (logica compartilhada)
  - Metodo privado `EnsureExchangeDeclaredAsync` com cache em `_declaredExchanges` (HashSet)
  - Declarar exchange como `ExchangeType.Topic` usando config de `RmqOptions.Exchanges` ou defaults
  - `BasicPublishAsync` com `exchange: exchangeName`, `routingKey: routingKey`, `mandatory: false`
  - Retry com Polly usando `DefaultRetry` das options
  - `RmqPublishException` em caso de falha apos retries (queueName = `{exchange}/{routingKey}`)
  - Logging: Debug em sucesso, Error em falha final
- Metodos existentes `PublishAsync` NAO devem ser alterados
- Testes unitarios com mocks
</requirements>

## Subtarefas

- [ ] 3.1 Adicionar assinaturas `PublishToTopicAsync` em `IRmqPublisher.cs`
- [ ] 3.2 Adicionar campo `_declaredExchanges` (HashSet<string>) em `RmqPublisher.cs`
- [ ] 3.3 Implementar `EnsureExchangeDeclaredAsync` com double-check locking
- [ ] 3.4 Implementar `PublishToTopicInternalAsync`
- [ ] 3.5 Implementar overloads publicos `PublishToTopicAsync` (sem e com headers)
- [ ] 3.6 Testes unitarios: publish na exchange com routing key corretos
- [ ] 3.7 Testes unitarios: exchange declarada como `topic` na primeira chamada
- [ ] 3.8 Testes unitarios: exchange NAO redeclarada em chamadas subsequentes (cache)
- [ ] 3.9 Testes unitarios: `mandatory=false` (diferente do publish direto que usa true)
- [ ] 3.10 Testes unitarios: retry acionado em falha transiente
- [ ] 3.11 Testes unitarios: `RmqPublishException` apos esgotar retries
- [ ] 3.12 Testes unitarios: publish com headers customizados
- [ ] 3.13 Testes unitarios: ExchangeOptions de `RmqOptions.Exchanges` usadas quando disponiveis
- [ ] 3.14 Validar que testes existentes do RmqPublisher continuam passando

## Sequenciamento

- Bloqueado por: 1.0, 2.0
- Desbloqueia: 7.0
- Paralelizavel: Sim (com 4.0)

## Detalhes de Implementacao

Ref: techspec secao 6.4 (RmqPublisher).

**Decisao: `mandatory: false`**
No pub/sub, eh valido publicar sem consumers inscritos. `mandatory: true` causaria `BasicReturn` exceptions. O `PublishAsync` existente (direto em queue) mantem `mandatory: true`.

**Cache de exchanges:**
```csharp
private readonly HashSet<string> _declaredExchanges = new(StringComparer.Ordinal);

private async Task EnsureExchangeDeclaredAsync(string exchangeName, CancellationToken ct)
{
    if (_declaredExchanges.Contains(exchangeName)) return;

    await _channelLock.WaitAsync(ct);
    try
    {
        if (_declaredExchanges.Contains(exchangeName)) return;

        var opts = _options.Exchanges.TryGetValue(exchangeName, out var o) ? o : new ExchangeOptions { Name = exchangeName };

        await _channel!.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Topic,
            durable: opts.Durable,
            autoDelete: opts.AutoDelete,
            arguments: ...,
            cancellationToken: ct);

        _declaredExchanges.Add(exchangeName);
    }
    finally { _channelLock.Release(); }
}
```

**Publish flow:**
1. Validar argumentos (exchangeName, routingKey, payload)
2. EnsureChannelAsync (reutiliza existente)
3. EnsureExchangeDeclaredAsync (novo)
4. CloudEventWrapper.Wrap(payload)
5. Retry loop: BasicPublishAsync com exchange e routingKey
6. Sucesso ou throw RmqPublishException

## Criterios de Sucesso

- `PublishToTopicAsync` publica na exchange correta com routing key correto
- Content-Type = `application/cloudevents+json`, DeliveryMode = Persistent
- `mandatory: false` no topic publish
- Exchange declarada apenas na primeira chamada (cache funciona)
- Retry executa ate MaxAttempts vezes em caso de falha
- `RmqPublishException` apos esgotar retries
- Headers customizados incluidos nas BasicProperties
- Metodos existentes `PublishAsync` nao alterados
- Testes existentes do Publisher continuam passando
