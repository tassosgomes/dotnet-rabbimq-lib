# Review: Task 2.0 - QueueManager — DeclareExchangeAndBindingsAsync

**Reviewer**: AI Code Reviewer  
**Date**: 2026-02-24  
**Task file**: tasks/exchange-topic/task_2.md  
**Status**: CHANGES REQUESTED  

## Summary

A tarefa 2.0 foi solicitada para implementar o método `DeclareExchangeAndBindingsAsync` no `QueueManager`, estendendo a interface `IQueueManager` e reutilizando o método existente `DeclareQueueWithDlqAsync`. No entanto, após análise do código, o método não foi implementado nem na interface nem na classe, e não há testes unitários correspondentes. A implementação está ausente, violando os requisitos da tarefa.

## Files Reviewed

| File | Status | Issues |
|------|--------|--------|
| src/Rmq.CloudEvents/Infrastructure/IQueueManager.cs | ⚠️ Issues | 1 |
| src/Rmq.CloudEvents/Infrastructure/QueueManager.cs | ⚠️ Issues | 1 |
| tests/Rmq.CloudEvents.Tests/Infrastructure/QueueManagerTests.cs | ⚠️ Issues | 8 |

## Issues Found

### 🔴 Critical Issues

- **Método DeclareExchangeAndBindingsAsync não implementado**: O método solicitado não foi adicionado à interface `IQueueManager.cs` nem implementado em `QueueManager.cs`. Isso quebra a funcionalidade esperada para declarar Topic Exchanges com bindings.
  - Arquivo: src/Rmq.CloudEvents/Infrastructure/IQueueManager.cs (linha N/A)
  - Sugestão: Adicionar a assinatura conforme especificado na tarefa:
    ```csharp
    Task DeclareExchangeAndBindingsAsync(
        IChannel channel,
        string exchangeName,
        string queueName,
        IReadOnlyList<string> bindingPatterns,
        QueueOptions queueOptions,
        ExchangeOptions? exchangeOptions = null,
        CancellationToken cancellationToken = default);
    ```
- **Implementação ausente em QueueManager.cs**: A classe `QueueManager` não possui o método implementado, incluindo validações, declaração de exchange como `ExchangeType.Topic`, reutilização de `DeclareQueueWithDlqAsync` e criação de bindings.
  - Arquivo: src/Rmq.CloudEvents/Infrastructure/QueueManager.cs (linha N/A)
  - Sugestão: Implementar o método com validações (channel não null, exchangeName/queueName não empty, bindingPatterns count > 0), declarar exchange com defaults (durable=true, autoDelete=false), reutilizar `DeclareQueueWithDlqAsync` e fazer QueueBindAsync para cada pattern.

### 🟡 Major Issues

- **Testes unitários ausentes**: Não há testes para o novo método. Os testes existentes cobrem apenas `DeclareQueueWithDlqAsync`.
  - Arquivo: tests/Rmq.CloudEvents.Tests/Infrastructure/QueueManagerTests.cs
  - Sugestão: Adicionar testes unitários conforme subtarefas 2.3 a 2.8, usando mocks para `IChannel` e verificando chamadas a `ExchangeDeclareAsync`, `DeclareQueueWithDlqAsync` (reutilização), `QueueBindAsync` para cada pattern, validações de entrada e defaults para `exchangeOptions`.
- **Violação de Clean Architecture**: A ausência do método quebra a arquitetura CQRS e Clean Architecture esperada, pois o QueueManager deveria gerenciar declarações de topologia.
  - Sugestão: Seguir os padrões arquiteturais do projeto, mantendo responsabilidades claras no Infrastructure layer.
- **Falta de validações**: Sem implementação, não há validações de entrada como requerido (channel null, strings vazias, bindingPatterns vazio).
  - Sugestão: Usar `ArgumentNullException.ThrowIfNull` e `ArgumentException.ThrowIfNullOrWhiteSpace` para validações.
- **Não reutiliza DeclareQueueWithDlqAsync**: O método existente não é chamado, violando o requisito de reutilização.
  - Sugestão: Chamar `await DeclareQueueWithDlqAsync(...)` dentro da implementação para declarar queue + DLQ.
- **Exchange não declarada como Topic**: Ausência de implementação significa que não declara `ExchangeType.Topic`.
  - Sugestão: Usar `await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Topic, ...)` com parâmetros de `exchangeOptions` ou defaults.
- **Bindings não criados**: Não há chamadas para `QueueBindAsync` para cada pattern.
  - Sugestão: Iterar sobre `bindingPatterns` e chamar `await channel.QueueBindAsync(queueName, exchangeName, pattern, ...)`.

### 🟢 Minor Issues

- **Falta de testes para cenários de erro**: Não há testes para bindingPatterns vazio ou parâmetros inválidos.
  - Sugestão: Adicionar testes que esperam `ArgumentException` ou `ArgumentNullException`.

## ✅ Positive Highlights

- O método existente `DeclareQueueWithDlqAsync` permanece inalterado, conforme requisito.
- A estrutura de testes existentes para `QueueManager` está bem organizada com mocks e verificações.

## Standards Compliance

| Standard | Status |
|----------|--------|
| C#/.NET | ⚠️ |
| REST/HTTP | N/A |
| Logging | N/A |
| Observability | N/A |
| Performance | N/A |
| Tests | ❌ |

## Recommendations

1. Implementar a assinatura do método em `IQueueManager.cs` exatamente como especificado na tarefa.
2. Implementar o método em `QueueManager.cs` com validações, declaração de exchange, reutilização de `DeclareQueueWithDlqAsync` e bindings para cada pattern.
3. Adicionar testes unitários abrangentes cobrindo todos os cenários (sucesso, validações, defaults).
4. Validar build com `dotnet build` e testes com `dotnet test`.
5. Garantir aderência aos padrões de Clean Architecture e CQRS do projeto.

## Verdict

A tarefa 2.0 não foi implementada, apesar do status em `tasks.md` indicar conclusão. O código está ausente, violando requisitos críticos como declaração de Topic Exchange e bindings. **CHANGES REQUESTED**: Implementar o método e testes antes de prosseguir para tarefas dependentes (3.0, 5.0). A ausência quebra a funcionalidade pub/sub esperada.