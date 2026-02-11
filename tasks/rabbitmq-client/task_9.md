## status: pending

<task_context>
<domain>infra/devops</domain>
<type>implementation</type>
<scope>configuration</scope>
<complexity>low</complexity>
<dependencies>task_7</dependencies>
</task_context>

# Tarefa 9.0: Sample Application e CI/CD

## Visao Geral

Criar uma aplicacao de exemplo funcional que demonstra todos os cenarios de uso da biblioteca (publish, consume, configuracao via DI) e configurar pipeline de CI com GitHub Actions.

<requirements>
- Implementar `samples/Rmq.CloudEvents.Sample/Program.cs`:
  - Registro via DI com `AddRmqCloudEvents`
  - Registro de consumer com `AddRmqConsumer`
  - Exemplo de publish de uma ordem
  - Exemplo de consumer handler que processa a ordem
  - Comentários explicativos para cada passo
- Criar `.github/workflows/ci.yml`:
  - Trigger: push/PR em main
  - Steps: checkout, setup-dotnet 8.0, restore, build Release, unit tests com cobertura, integration tests, pack (condicional em main)
</requirements>

## Subtarefas

- [x] 9.1 Implementar `Program.cs` do sample com cenarios completos (DI, publish, consume)
- [x] 9.2 Criar `.github/workflows/ci.yml`
- [x] 9.3 Validar que o sample compila corretamente

## Detalhes de Implementacao

Ref: techspec secoes 6.2-6.4 (Cenarios de uso) e 17 (CI/CD).

**Sample Program.cs** deve demonstrar:
```csharp
// 1. Configuracao
services.AddRmqCloudEvents(options => { ... });
services.AddRmqConsumer<Order, OrderConsumer>("orders");

// 2. Publish
await publisher.PublishAsync("orders", new Order { Id = 1, Total = 99.90m });

// 3. Consumer handler
public class OrderConsumer : IRmqMessageHandler<Order> { ... }
```

**CI Pipeline**:
```yaml
- dotnet restore
- dotnet build --no-restore -c Release
- dotnet test tests/Rmq.CloudEvents.Tests -c Release --collect:"XPlat Code Coverage"
- dotnet test tests/Rmq.CloudEvents.IntegrationTests -c Release
- dotnet pack src/Rmq.CloudEvents -c Release -o ./artifacts (apenas em main)
```

## Critérios de Sucesso

- Sample compila sem erros
- Sample demonstra uso claro e conciso da API publica
- CI pipeline executa build, testes e pack com sucesso
- Pipeline usa dotnet 8.0.x
