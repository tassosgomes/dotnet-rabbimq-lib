## status: done

<task_context>
<domain>infra/di</domain>
<type>implementation</type>
<scope>middleware</scope>
<complexity>medium</complexity>
<dependencies>task_5, task_6</dependencies>
</task_context>

# Tarefa 7.0: Dependency Injection e ServiceCollection Extensions

## Visao Geral

Implementar os metodos de extensao para `IServiceCollection` que registram todos os servicos da biblioteca no container de DI do .NET. Deve permitir configuracao fluente e registro de consumers por queue.

<requirements>
- Implementar `Extensions/ServiceCollectionExtensions.cs`:
  - `AddRmqCloudEvents(Action<RmqOptions>)`: registra ConnectionManager (Singleton), QueueManager (Singleton), CloudEventWrapper (Singleton), MessageSerializer (Singleton), Publisher (Transient)
  - `AddRmqConsumer<TMessage, THandler>(string queueName)`: registra handler (Transient) e RmqConsumer<T> como HostedService
- Validacao de argumentos (configure nao null, queueName nao vazio)
- Testes unitários verificando registros corretos no container
</requirements>

## Subtarefas

- [x] 7.1 Implementar `AddRmqCloudEvents` extension method
- [x] 7.2 Implementar `AddRmqConsumer<TMessage, THandler>` extension method
- [x] 7.3 Testes unitários: verificar que todos os servicos sao resolvidos corretamente
- [x] 7.4 Testes unitários: validacao de argumentos (null configure, queueName vazio)

## Detalhes de Implementacao

Ref: techspec secao 11 (Registro via Dependency Injection).

**Lifecycles**:
| Componente | Lifecycle |
|---|---|
| IRmqConnectionManager | Singleton |
| IQueueManager | Singleton |
| ICloudEventWrapper | Singleton |
| IMessageSerializer | Singleton |
| IRmqPublisher | Transient |
| IRmqMessageHandler<T> | Transient |
| RmqConsumer<T> | HostedService |

O `AddRmqConsumer` registra um `IHostedService` que resolve as dependencias do container e inicia o consumo automaticamente.

**Uso esperado**:
```csharp
services.AddRmqCloudEvents(options => {
    options.Connection = new RmqConnectionOptions { HostName = "localhost" };
    options.DefaultCloudEvents = new CloudEventsOptions { Source = new Uri("/my-service", UriKind.Relative) };
});
services.AddRmqConsumer<Order, OrderConsumer>("orders");
```

## Critérios de Sucesso

- `AddRmqCloudEvents` registra todos os servicos internos
- `IRmqPublisher` eh resolvivel apos registro
- `AddRmqConsumer` registra handler e HostedService
- Argumentos nulos/vazios lancam `ArgumentException`
- Build sem warnings
