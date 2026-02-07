## status: completed

<task_context>
<domain>infra/conexao</domain>
<type>implementation</type>
<scope>core_feature</scope>
<complexity>medium</complexity>
<dependencies>task_2</dependencies>
</task_context>

# Tarefa 4.0: Connection Manager e Queue Manager

## Visao Geral

Implementar o gerenciamento de conexao com RabbitMQ (singleton, thread-safe, auto-recovery) e o gerenciamento de declaracao de quorum queues com DLQ associada (DLX + bind).

<requirements>
- Implementar `Connection/IRmqConnectionManager.cs` (GetConnectionAsync, CreateChannelAsync)
- Implementar `Connection/RmqConnectionManager.cs`:
  - Singleton lifecycle, protegido por SemaphoreSlim (double-check locking)
  - AutomaticRecoveryEnabled = true, TopologyRecoveryEnabled = true
  - NetworkRecoveryInterval configurável
  - Suporte a SSL/TLS via SslOption
  - IAsyncDisposable para cleanup
- Implementar `Infrastructure/IQueueManager.cs` (DeclareQueueWithDlqAsync)
- Implementar `Infrastructure/QueueManager.cs`:
  - Declara DLX (exchange direct, durable)
  - Declara DLQ como quorum queue
  - Bind DLQ ao DLX com routing key = queueName
  - Declara queue principal como quorum com x-dead-letter-exchange, x-dead-letter-routing-key, x-delivery-limit, x-quorum-initial-group-size
- Testes unitários com mocks de IConnection/IChannel
</requirements>

## Subtarefas

- [x] 4.1 Implementar `IRmqConnectionManager` e `RmqConnectionManager`
- [x] 4.2 Implementar `IQueueManager` e `QueueManager`
- [x] 4.3 Testes unitários para `RmqConnectionManager` (reuso de conexao, criacao de canal, comportamento com conexao fechada)
- [x] 4.4 Testes unitários para `QueueManager` (argumentos corretos em QueueDeclareAsync, ExchangeDeclareAsync, QueueBindAsync, nomenclatura DLQ/DLX)

## Detalhes de Implementacao

Ref: techspec secoes 8.1 (RmqConnectionManager) e 8.2 (QueueManager).

**ConnectionManager**: usa `ConnectionFactory` do RabbitMQ.Client 7.x com `CreateConnectionAsync`. Conexao reutilizada via double-check locking com `SemaphoreSlim`. Canais criados sob demanda.

**QueueManager**: sequencia de declaracao:
1. `ExchangeDeclareAsync` - DLX (`{queueName}.dlx`, type=direct, durable=true)
2. `QueueDeclareAsync` - DLQ (`{queueName}.dlq`, x-queue-type=quorum)
3. `QueueBindAsync` - DLQ ao DLX (routing key = queueName)
4. `QueueDeclareAsync` - Queue principal (x-queue-type=quorum, x-dead-letter-exchange, x-dead-letter-routing-key, x-delivery-limit)

## Critérios de Sucesso

- ConnectionManager reutiliza conexao existente se aberta
- ConnectionManager cria nova conexao se anterior esta fechada
- QueueManager declara DLX, DLQ e queue principal com argumentos corretos
- DLQ nomeada como `{queueName}{suffix}` (default: `.dlq`)
- Testes verificam todos os argumentos passados via mock
- Build sem warnings
