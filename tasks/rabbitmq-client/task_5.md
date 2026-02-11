## status: done

<task_context>
<domain>engine/messaging</domain>
<type>implementation</type>
<scope>core_feature</scope>
<complexity>high</complexity>
<dependencies>task_3, task_4</dependencies>
</task_context>

# Tarefa 5.0: Publisher com Retry Exponencial

## Visao Geral

Implementar o componente de publicacao de mensagens com encapsulamento transparente em CloudEvents e retry exponencial via Polly. Inclui interface publica `IRmqPublisher` e implementacao `RmqPublisher`.

<requirements>
- Implementar `Publishing/IRmqPublisher.cs` (IAsyncDisposable):
  - `PublishAsync<T>(queueName, payload, cloudEventType?, cancellationToken)`
  - `PublishAsync<T>(queueName, payload, headers, cloudEventType?, cancellationToken)`
- Implementar `Publishing/RmqPublisher.cs`:
  - Encapsula payload via ICloudEventWrapper.Wrap
  - Retry com Polly ResiliencePipeline (exponential backoff com jitter)
  - Handles: RabbitMQClientException, IOException, TimeoutException
  - BasicPublishAsync com content-type=application/cloudevents+json, DeliveryMode=Persistent
  - Garante canal aberto (EnsureChannelAsync)
  - Declara queue via QueueManager antes do primeiro publish
  - Suporte a headers customizados via BasicProperties
  - Logging: Debug em sucesso, Warning em retry, Error apos esgotar
- Testes unitários com mocks
</requirements>

## Subtarefas

- [x] 5.1 Implementar `IRmqPublisher` interface
- [x] 5.2 Implementar `RmqPublisher` com logica de publish e retry
- [x] 5.3 Implementar metodo privado `BuildRetryPipeline` (Polly 8.x ResiliencePipelineBuilder)
- [x] 5.4 Testes unitários: publish com sucesso (verifica BasicPublishAsync chamado corretamente, content-type, body)
- [x] 5.5 Testes unitários: retry acionado em falha transiente
- [x] 5.6 Testes unitários: excecao propagada apos max retries
- [x] 5.7 Testes unitários: publish com headers customizados

## Detalhes de Implementacao

Ref: techspec secoes 8.4 (RmqPublisher) e fluxo 12.1 (Publish Flow).

**Retry Pipeline** (Polly 8.x):
```
ResiliencePipelineBuilder()
  .AddRetry(new RetryStrategyOptions {
    ShouldHandle = Handle<RabbitMQClientException>().Handle<IOException>().Handle<TimeoutException>(),
    MaxRetryAttempts = options.MaxAttempts,
    Delay = options.InitialDelay,
    BackoffType = DelayBackoffType.Exponential,
    UseJitter = options.UseJitter,
    OnRetry = logging
  })
```

**Publish Flow**:
1. Validar argumentos (queueName, payload)
2. EnsureChannelAsync (cria canal se nao existe)
3. DeclareQueueWithDlqAsync (idempotente)
4. CloudEventWrapper.Wrap(payload)
5. Retry loop: BasicPublishAsync com properties
6. Sucesso ou throw RmqPublishException

## Critérios de Sucesso

- Mensagem publicada contem CloudEvent valido no body
- Content-Type = `application/cloudevents+json`
- DeliveryMode = Persistent
- Retry executa até MaxAttempts vezes em caso de falha
- Apos esgotar retries, excecao eh propagada
- Headers customizados sao incluidos nas BasicProperties
- Testes cobrem cenarios de sucesso, retry e falha final
