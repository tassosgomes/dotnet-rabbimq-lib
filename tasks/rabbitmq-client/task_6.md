## status: pending

<task_context>
<domain>engine/messaging</domain>
<type>implementation</type>
<scope>core_feature</scope>
<complexity>high</complexity>
<dependencies>task_3, task_4</dependencies>
</task_context>

# Tarefa 6.0: Consumer com Retry e ACK/NACK

## Visao Geral

Implementar o componente de consumo de mensagens com desencapsulamento transparente de CloudEvents, retry no handler do desenvolvedor via Polly, e ACK/NACK automático. Inclui `IRmqConsumer`, `RmqConsumer` (HostedService), `RmqAsyncConsumerHandler`, `IRmqMessageHandler<T>` e `MessageContext`.

<requirements>
- Implementar `Consuming/MessageContext.cs` (EventId, Source, EventType, Timestamp, Headers, DeliveryTag, QueueName, AttemptNumber)
- Implementar `Consuming/IRmqMessageHandler.cs` (interface generica HandleAsync(T message, MessageContext, CancellationToken))
- Implementar `Consuming/IRmqConsumer.cs` (StartAsync, StopAsync, IAsyncDisposable)
- Implementar `Consuming/RmqAsyncConsumerHandler.cs`:
  - Herda `AsyncDefaultBasicConsumer` do RabbitMQ.Client 7.x
  - HandleBasicDeliverAsync: unwrap CloudEvent -> monta MessageContext -> retry loop com handler -> ACK ou NACK
  - Em sucesso: BasicAckAsync
  - Em falha apos retries: BasicNackAsync(requeue: false) para DLQ
  - Logging adequado
- Implementar `Consuming/RmqConsumer.cs`:
  - Implementa IHostedService (BackgroundService ou manual)
  - StartAsync: cria canal, declara queue via QueueManager, registra consumer handler
  - StopAsync: cancela consumo, fecha canal
- Testes unitários com mocks para ambos os componentes
</requirements>

## Subtarefas

- [x] 6.1 Implementar `MessageContext`
- [x] 6.2 Implementar `IRmqMessageHandler<T>` interface
- [x] 6.3 Implementar `IRmqConsumer` interface
- [x] 6.4 Implementar `RmqAsyncConsumerHandler<T>` (unwrap, retry, ACK/NACK)
- [x] 6.5 Implementar `RmqConsumer<T>` (HostedService lifecycle)
- [x] 6.6 Testes unitários para `RmqAsyncConsumerHandler` (ACK sucesso, NACK falha, retry no handler, unwrap correto)
- [x] 6.7 Testes unitários para `RmqConsumer` (start/stop lifecycle, declaracao de queue)

## Detalhes de Implementacao

Ref: techspec secoes 8.5 (RmqConsumer e RmqAsyncConsumerHandler) e fluxo 12.2 (Consume Flow).

**Consumer Handler Flow**:
1. Recebe mensagem via `HandleBasicDeliverAsync`
2. `ICloudEventWrapper.Unwrap<T>(body)` -> payload + metadata
3. Monta `MessageContext` com metadata + delivery info
4. Retry loop (Polly): `handler.HandleAsync(payload, context, ct)`
5. Sucesso: `BasicAckAsync(deliveryTag, multiple: false)`
6. Falha final: `BasicNackAsync(deliveryTag, multiple: false, requeue: false)` -> DLQ via DLX

**RmqConsumer Lifecycle**:
- StartAsync: GetConnection -> CreateChannel -> DeclareQueueWithDlq -> BasicConsumeAsync com handler
- StopAsync: BasicCancelAsync -> CloseChannel

## Critérios de Sucesso

- Handler do desenvolvedor recebe payload puro (sem CloudEvents)
- MessageContext contem metadados corretos (EventId, Source, etc.)
- ACK enviado em processamento com sucesso
- NACK (requeue=false) enviado apos esgotar retries -> mensagem vai para DLQ
- Consumer inicia e para corretamente como HostedService
- Testes cobrem cenarios de sucesso, falha e lifecycle
