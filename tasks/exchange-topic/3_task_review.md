# Review: Task 3.0 - Publisher — PublishToTopicAsync

**Reviewer**: AI Code Reviewer
**Date**: 2026-02-24
**Task file**: tasks/exchange-topic/task_3.md
**Status**: CHANGES REQUESTED

## Summary

A implementação da Task 3.0 - Publisher — PublishToTopicAsync não foi realizada. Os novos métodos `PublishToTopicAsync` não foram adicionados à interface `IRmqPublisher` nem implementados na classe `RmqPublisher`. O campo `_declaredExchanges` e os métodos auxiliares `EnsureExchangeDeclaredAsync` e `PublishToTopicInternalAsync` também não foram implementados. Isso representa uma violação crítica dos requisitos da tarefa, impedindo o progresso das tarefas subsequentes (4.0, 5.0, etc.).

## Files Reviewed

| File | Status | Issues |
|------|--------|--------|
| tasks/exchange-topic/task_3.md | ✅ OK | 0 |
| src/Rmq.CloudEvents/Publishing/IRmqPublisher.cs | ❌ Problems | 2 |
| src/Rmq.CloudEvents/Publishing/RmqPublisher.cs | ❌ Problems | 5 |
| tests/Rmq.CloudEvents.Tests/Publishing/RmqPublisherTests.cs | ⚠️ Issues | 13 |

## Issues Found

### 🔴 Critical Issues

- **Implementação não realizada**: A Task 3.0 não foi implementada. Os métodos `PublishToTopicAsync<T>(exchangeName, routingKey, payload, cloudEventType?, cancellationToken)` e `PublishToTopicAsync<T>(exchangeName, routingKey, payload, headers, cloudEventType?, cancellationToken)` não existem na interface `IRmqPublisher`.
- **Campo _declaredExchanges ausente**: O campo `private readonly HashSet<string> _declaredExchanges` não foi adicionado à classe `RmqPublisher`, violando o requisito de cache de exchanges declaradas.
- **Método EnsureExchangeDeclaredAsync ausente**: O método privado `EnsureExchangeDeclaredAsync` com double-check locking não foi implementado.
- **Método PublishToTopicInternalAsync ausente**: O método privado `PublishToTopicInternalAsync` para lógica compartilhada não foi implementado.
- **Testes não implementados**: Nenhum dos 13 testes unitários especificados na subtarefa foi implementado, incluindo testes para publish em exchange, cache, retry, exceptions e headers.

### 🟡 Major Issues

- **Mantém API existente intacta**: Embora os métodos existentes `PublishAsync` não tenham sido alterados (cumprindo o requisito), isso não compensa a ausência da nova funcionalidade.

### 🟢 Minor Issues

- Nenhum identificado, pois a implementação não foi realizada.

## ✅ Positive Highlights

- A estrutura de código existente segue boas práticas de qualidade.
- Os métodos `PublishAsync` existentes permanecem inalterados, conforme requerido.

## Standards Compliance

| Standard | Status |
|----------|--------|
| C#/.NET | ❌ |
| REST/HTTP | N/A |
| Logging | N/A |
| Observability | N/A |
| Performance | N/A |
| Tests | ❌ |

## Recommendations

1. **Implementar imediatamente os métodos na interface IRmqPublisher**:
   - Adicionar as duas sobrecargas de `PublishToTopicAsync` conforme especificado na tarefa.

2. **Implementar o cache de exchanges declaradas**:
   - Adicionar campo `private readonly HashSet<string> _declaredExchanges = new(StringComparer.Ordinal);`
   - Implementar `EnsureExchangeDeclaredAsync` com double-check locking usando `_channelLock`.

3. **Implementar PublishToTopicInternalAsync**:
   - Método privado compartilhado entre as duas sobrecargas públicas.
   - Seguir o fluxo: validar argumentos → EnsureChannelAsync → EnsureExchangeDeclaredAsync → CloudEventWrapper.Wrap → retry loop com BasicPublishAsync(exchangeName, routingKey, mandatory: false).

4. **Configurar ExchangeType.Topic**:
   - Usar `_options.Exchanges.TryGetValue(exchangeName, out var opts)` ou defaults.
   - Declarar exchange com `ExchangeDeclareAsync(type: ExchangeType.Topic)`.

5. **Implementar retry com Polly**:
   - Usar `DefaultRetry` das opções.
   - Capturar falhas e lançar `RmqPublishException` com queueName = `{exchange}/{routingKey}`.

6. **Adicionar logging**:
   - Debug em sucesso, Error em falha final.

7. **Implementar todos os 13 testes unitários**:
   - Usar mocks para `IChannel`, `IRmqConnectionManager`, etc.
   - Testar exchange declarada como `topic`, cache funcionando, `mandatory=false`, retry acionado, headers customizados, etc.

8. **Validar testes existentes continuam passando**.

## Código Implementado

Como a implementação não foi realizada, não há código a revisar.

## Testes

Não foram implementados os testes especificados.

## Padrões Seguidos

Os padrões do projeto não foram aplicados, pois a implementação não foi feita.

## Pontos de Melhoria

A implementação completa da tarefa conforme especificado.

## Conclusão

A Task 3.0 deve ser implementada completamente antes de prosseguir. A ausência dessa funcionalidade bloqueia o desenvolvimento do suporte a Topic Exchange no projeto.</content>
<parameter name="filePath">tasks/exchange-topic/3_task_review.md