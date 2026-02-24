---
status: pending
parallelizable: false
blocked_by: [3.0, 6.0]
---

<task_context>
<domain>engine/messaging</domain>
<type>integration|testing</type>
<scope>core_feature</scope>
<complexity>high</complexity>
<dependencies>task_3, task_6</dependencies>
<unblocks>""</unblocks>
</task_context>

# Tarefa 7.0: Testes de Integracao e Sample Application

## Visao Geral

Criar testes de integracao com Testcontainers que validam o fluxo completo de pub/sub via Topic Exchange contra um RabbitMQ real. Atualizar o sample application para demonstrar o cenario pub/sub com multiplos consumers inscritos em topicos diferentes.

<requirements>
- Criar `tests/Rmq.CloudEvents.IntegrationTests/TopicExchangeTests.cs`:
  - Roundtrip pub/sub: publish em exchange → consumer recebe mensagem correta
  - Binding seletivo: consumer com `orders.*` recebe `orders.created` mas nao `payments.completed`
  - Binding `#`: consumer recebe todas as mensagens da exchange
  - Multiplos bindings: consumer com `["orders.*", "payments.*"]` recebe de ambos
  - Multiplos consumers mesma exchange: dois consumers com patterns diferentes recebem mensagens corretas
  - DLQ via topic: mensagem que falha no handler vai para DLQ
  - CloudEvents no wire: mensagem publicada via topic contem CloudEvent valido
  - Coexistencia: `PublishAsync` direto em queue + `PublishToTopicAsync` funcionam simultaneamente
  - MessageContext: `ExchangeName` e `RoutingKey` preenchidos corretamente no handler
- Atualizar `samples/Rmq.CloudEvents.Sample/Program.cs`:
  - Adicionar cenario pub/sub alem do existente (direto em queue)
  - Registrar exchange topic
  - Publicar com routing keys diferentes
  - Dois consumers inscritos em patterns diferentes
- Validacao final:
  - `dotnet build` Release sem warnings
  - Todos os testes unitarios passam
  - Todos os testes de integracao passam
</requirements>

## Subtarefas

- [ ] 7.1 Criar `TopicExchangeTests.cs` — roundtrip pub/sub basico
- [ ] 7.2 Teste: binding seletivo (consumer `orders.*` nao recebe `payments.*`)
- [ ] 7.3 Teste: binding `#` recebe tudo
- [ ] 7.4 Teste: multiplos bindings em um consumer
- [ ] 7.5 Teste: multiplos consumers na mesma exchange com patterns diferentes
- [ ] 7.6 Teste: DLQ routing via topic exchange
- [ ] 7.7 Teste: CloudEvents valido no wire (formato da mensagem)
- [ ] 7.8 Teste: coexistencia publish direto + topic na mesma aplicacao
- [ ] 7.9 Teste: MessageContext.ExchangeName e RoutingKey corretos
- [ ] 7.10 Atualizar `Program.cs` do sample com cenario pub/sub
- [ ] 7.11 Validar `dotnet build -c Release` e `dotnet test` completo
- [ ] 7.12 Validar que testes de integracao existentes (PublishConsumeTests, DlqTests) continuam passando

## Sequenciamento

- Bloqueado por: 3.0, 6.0 (todos os componentes devem estar prontos)
- Desbloqueia: Nenhum (tarefa final)
- Paralelizavel: Nao (tarefa de validacao final)

## Detalhes de Implementacao

Ref: techspec secao 10 (Estrategia de Testes) e secao 5 (Cenarios de Uso).

**Fixture (reutilizar existente):**
O `RabbitMqFixture` existente em `Fixtures/` ja levanta um container RabbitMQ. Os novos testes devem reutiliza-lo.

**Exemplo de teste — roundtrip basico:**
```csharp
[Fact]
public async Task PublishToTopic_ConsumerReceivesMessage()
{
    // Arrange
    var exchangeName = "test-events";
    var routingKey = "orders.created";
    var queueName = "test-orders-queue";
    var order = new TestOrder { Id = 1, Name = "Test" };

    var received = new TaskCompletionSource<TestOrder>();
    // Setup consumer with binding "orders.*" on queueName
    // Setup publisher

    // Act
    await publisher.PublishToTopicAsync(exchangeName, routingKey, order);

    // Assert
    var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
    Assert.Equal(order.Id, result.Id);
}
```

**Exemplo de teste — binding seletivo:**
```csharp
[Fact]
public async Task TopicConsumer_OnlyReceivesMatchingRoutingKeys()
{
    // Consumer bind: "orders.*"
    // Publish 1: routingKey "orders.created" → recebida
    // Publish 2: routingKey "payments.completed" → NAO recebida
    // Assert: consumer recebeu exatamente 1 mensagem
}
```

**Sample Application (adicao):**
```csharp
// Novo cenario adicionado ao Program.cs existente:
options.Exchanges.Add("business-events", new ExchangeOptions
{
    Name = "business-events"
});

services.AddRmqTopicConsumer<OrderCreated, OrderAuditConsumer>(opts =>
{
    opts.ExchangeName = "business-events";
    opts.QueueName = "order-audit";
    opts.BindingPatterns = ["orders.*"];
});

// Publish via topic
await publisher.PublishToTopicAsync("business-events", "orders.created", order);
```

## Criterios de Sucesso

- Todos os 9 cenarios de integracao passam
- Testes de integracao existentes (PublishConsumeTests, DlqTests) continuam passando
- Sample application compila e demonstra pub/sub + queue direta
- `dotnet build -c Release` sem warnings
- `dotnet test` completo sem falhas
