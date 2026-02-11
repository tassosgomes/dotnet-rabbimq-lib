## status: pending

<task_context>
<domain>engine/serializacao</domain>
<type>implementation</type>
<scope>core_feature</scope>
<complexity>medium</complexity>
<dependencies>task_2</dependencies>
</task_context>

# Tarefa 3.0: Serializacao e CloudEvents Wrapper

## Visao Geral

Implementar a camada de serializacao JSON e o componente CloudEventWrapper que encapsula/desencapsula payloads em formato CloudEvents de forma transparente. Estes sao componentes internos fundamentais usados pelo Publisher e Consumer.

<requirements>
- Implementar `Serialization/IMessageSerializer.cs` com métodos `Serialize<T>` e `Deserialize<T>`
- Implementar `Serialization/SystemTextJsonMessageSerializer.cs` com camelCase e ignore null
- Implementar `CloudEvents/CloudEventMetadata.cs` (record com EventId, Source, EventType, Timestamp)
- Implementar `CloudEvents/ICloudEventWrapper.cs` (interface interna com Wrap/Unwrap)
- Implementar `CloudEvents/CloudEventWrapper.cs`:
  - Wrap: cria CloudEvent com id (Guid), source, type, time, datacontenttype=application/json, data
  - Unwrap: extrai payload tipado e CloudEventMetadata
  - Usa `CloudNative.CloudEvents.SystemTextJson.JsonEventFormatter`
  - Modo structured content mode (JSON)
- Testes unitários cobrindo roundtrip, campos obrigatórios, tipos diversos, eventType customizado
</requirements>

## Subtarefas

- [x] 3.1 Implementar `IMessageSerializer` e `SystemTextJsonMessageSerializer`
- [x] 3.2 Implementar `CloudEventMetadata` record
- [x] 3.3 Implementar `ICloudEventWrapper` interface
- [x] 3.4 Implementar `CloudEventWrapper` (Wrap e Unwrap)
- [x] 3.5 Testes unitários para `SystemTextJsonMessageSerializer` (roundtrip, null, tipos complexos)
- [x] 3.6 Testes unitários para `CloudEventWrapper` (wrap/unwrap roundtrip, campos obrigatórios, eventType customizado, falha em payload inválido)

## Detalhes de Implementacao

Ref: techspec secoes 8.3 (CloudEventWrapper) e 8.6 (Serialization).

O `CloudEventWrapper` deve usar `JsonEventFormatter` do SDK CloudNative para encode/decode. O `Wrap` gera bytes em structured mode (`EncodeStructuredModeMessage`). O `Unwrap` decodifica e trata `Data` como `JsonElement` para deserializar via `System.Text.Json`.

Content-Type da mensagem AMQP sera `application/cloudevents+json`.

Exemplo de CloudEvent gerado:
```json
{
  "specversion": "1.0",
  "id": "f47ac10b-...",
  "source": "/my-service",
  "type": "com.mycompany.order.created.v1",
  "time": "2026-02-07T14:30:00Z",
  "datacontenttype": "application/json",
  "data": { "orderId": 12345 }
}
```

## Critérios de Sucesso

- Serialize/Deserialize roundtrip preserva dados fielmente
- CloudEvent Wrap/Unwrap roundtrip preserva payload e metadata
- Todos os campos obrigatórios do CloudEvent estao presentes (specversion, id, source, type, time)
- Testes passam com tipos simples e complexos (objetos aninhados, listas)
- >= 90% cobertura nos componentes testados
