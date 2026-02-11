## status: pending

<task_context>
<domain>engine/testing</domain>
<type>testing</type>
<scope>core_feature</scope>
<complexity>high</complexity>
<dependencies>task_7</dependencies>
</task_context>

# Tarefa 8.0: Testes de Integracao com Testcontainers

## Visao Geral

Implementar testes de integracao end-to-end usando Testcontainers para levantar um RabbitMQ real. Validar os fluxos completos de publish/consume, CloudEvents no wire e roteamento para DLQ.

<requirements>
- Implementar `Fixtures/RabbitMqFixture.cs` com Testcontainers (rabbitmq:3.13-management)
- Teste: publish + consume roundtrip (payload intacto apos wrap/unwrap)
- Teste: CloudEvents no wire (mensagem no RabbitMQ contem CloudEvent JSON valido com todos os campos obrigatorios)
- Teste: DLQ routing (handler que falha N vezes resulta em mensagem na DLQ com CloudEvent preservado)
- Teste: multiplas queues independentes (consumers em queues diferentes funcionam sem interferencia)
</requirements>

## Subtarefas

- [x] 8.1 Implementar `RabbitMqFixture` (IAsyncLifetime, container RabbitMQ, expoe ConnectionString)
- [x] 8.2 Teste de integracao: publish + consume roundtrip com payload complexo
- [x] 8.3 Teste de integracao: validar formato CloudEvent na mensagem raw do RabbitMQ
- [x] 8.4 Teste de integracao: DLQ routing apos falhas no handler
- [x] 8.5 Teste de integracao: multiplas queues com consumers independentes

## Detalhes de Implementacao

Ref: techspec secoes 16.1-16.3 (Estrategia de Testes).

**RabbitMqFixture**:
```csharp
public sealed class RabbitMqFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder()
        .WithImage("rabbitmq:3.13-management")
        .Build();
    public string ConnectionString => _container.GetConnectionString();
    public async Task InitializeAsync() => await _container.StartAsync();
    public async Task DisposeAsync() => await _container.DisposeAsync();
}
```

**Cenarios de teste**:
1. **Roundtrip**: publica Order -> consume -> verifica campos iguais
2. **CloudEvents wire**: publica -> le mensagem raw via RabbitMQ client direto -> valida JSON CloudEvent
3. **DLQ**: handler lanca excecao sempre -> apos retries, mensagem aparece na DLQ
4. **Multi-queue**: publica em queue-a e queue-b -> cada consumer recebe apenas sua mensagem

## Critérios de Sucesso

- Todos os testes passam com container RabbitMQ real
- Roundtrip preserva payload fielmente
- Mensagem no wire eh CloudEvent valido com specversion, id, source, type, time, data
- Mensagem na DLQ mantem formato CloudEvent
- Testes sao isolados e idempotentes
