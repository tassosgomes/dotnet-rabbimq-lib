# Task 4.0 Review - MessageContext e RmqAsyncConsumerHandler — Exchange/RoutingKey

## Resumo da Implementação
Implementada extensão do MessageContext para incluir propriedades ExchangeName e RoutingKey, permitindo handlers consumerem mensagens com contexto completo de exchange/routing key. Modificado RmqAsyncConsumerHandler para repassar esses valores.

## Alterações Realizadas
- **MessageContext.cs**: Adicionadas propriedades `ExchangeName` e `RoutingKey` com valores padrão vazios para backward compatibility.
- **RmqAsyncConsumerHandler.cs**: Atualizado método `CreateMessageContext` para incluir exchange e routingKey do evento de entrega RabbitMQ.

## Padrões de Código Seguidos
- Extensão de classes existentes com `init` properties e valores padrão.
- Modificação de métodos privados com atualização de todas as chamadas.
- Documentação XML detalhada para propriedades de contexto.

## Validações
- Backward compatibility: Handlers existentes continuam funcionando com valores vazios.
- Testes unitários cobrem cenários novos e antigos.
- Padrões RabbitMQ respeitados: ExchangeName vazio para default exchange, RoutingKey com nome da queue.

## Notas sobre padrões de código descobertos nesta revisão:
- **Extensão de classes existentes**: Quando adicionar novas propriedades a classes existentes, usar `init` properties com valores padrão (`string.Empty`) garante backward-compatibility. Exemplo: `public string ExchangeName { get; init; } = string.Empty;`
- **Modificação de métodos privados**: Ao alterar assinatura de métodos privados como `CreateMessageContext`, atualizar todas as chamadas para incluir os novos parâmetros. No caso do RabbitMQ, `exchange` e `routingKey` são parâmetros disponíveis no `HandleBasicDeliverAsync`.
- **Testes para backward-compatibility**: Adicionar testes específicos que validam que handlers existentes continuam funcionando com valores padrão vazios, além de testes para a nova funcionalidade.
- **Documentação XML**: Propriedades de contexto devem ter comentários detalhados explicando quando ficam vazias (ex: "Vazio para mensagens da default exchange").
- **Padrões RabbitMQ**: Em consumo direto de queue (sem topic exchange), `ExchangeName` fica vazio e `RoutingKey` contém o nome da queue. Em topic exchanges, ambos têm valores específicos.
