---
status: pending
parallelizable: false
blocked_by: []
---

<task_context>
<domain>infra/messaging</domain>
<type>implementation</type>
<scope>configuration</scope>
<complexity>low</complexity>
<dependencies>nenhuma</dependencies>
<unblocks>"2.0, 3.0, 4.0"</unblocks>
</task_context>

# Tarefa 1.0: Modelos de Configuracao (ExchangeOptions, TopicSubscriptionOptions, RmqOptions.Exchanges)

## Visao Geral

Criar os novos modelos de configuracao necessarios para Topic Exchange e estender `RmqOptions` com a nova propriedade `Exchanges`. Essas classes sao o alicerce de todas as tarefas subsequentes. Nenhuma alteracao em codigo existente deve quebrar a API atual.

<requirements>
- Criar `Configuration/ExchangeOptions.cs`:
  - `Name` (required string) — nome da exchange
  - `Durable` (bool, default true) — sobrevive a restart do broker
  - `AutoDelete` (bool, default false) — deletada quando sem bindings
  - `Arguments` (IDictionary<string, object>?, default null) — argumentos extras
  - O tipo da exchange NAO eh exposto (sempre topic neste escopo)
- Criar `Configuration/TopicSubscriptionOptions.cs`:
  - `ExchangeName` (required string) — nome da exchange topic
  - `QueueName` (string?, recomendado nomes fixos para durabilidade)
  - `BindingPatterns` (required IReadOnlyList<string>) — patterns com wildcards (* e #)
  - `Queue` (QueueOptions, default new()) — config de quorum, retry, DLQ
- Estender `Configuration/RmqOptions.cs`:
  - Nova propriedade `Exchanges` (Dictionary<string, ExchangeOptions>, default new())
  - Propriedades existentes devem permanecer inalteradas
- Testes unitarios para defaults, validacao e backward-compatibility
</requirements>

## Subtarefas

- [ ] 1.1 Criar `Configuration/ExchangeOptions.cs`
- [ ] 1.2 Criar `Configuration/TopicSubscriptionOptions.cs`
- [ ] 1.3 Adicionar propriedade `Exchanges` em `RmqOptions.cs`
- [ ] 1.4 Testes unitarios: `ExchangeOptionsTests.cs` (defaults, propriedades)
- [ ] 1.5 Testes unitarios: `TopicSubscriptionOptionsTests.cs` (defaults, propriedades)
- [ ] 1.6 Testes unitarios: `RmqOptions` — propriedade `Exchanges` inicializada vazia, nao afeta config existente
- [ ] 1.7 Validar build (`dotnet build`)

## Sequenciamento

- Bloqueado por: Nenhum
- Desbloqueia: 2.0, 3.0, 4.0
- Paralelizavel: Nao (eh a primeira tarefa)

## Detalhes de Implementacao

Ref: techspec secoes 4.2.1 (ExchangeOptions), 4.2.2 (TopicSubscriptionOptions), 4.2.3 (RmqOptions.Exchanges).

**ExchangeOptions:**
```csharp
public sealed class ExchangeOptions
{
    public required string Name { get; set; }
    public bool Durable { get; set; } = true;
    public bool AutoDelete { get; set; } = false;
    public IDictionary<string, object>? Arguments { get; set; }
}
```

**TopicSubscriptionOptions:**
```csharp
public sealed class TopicSubscriptionOptions
{
    public required string ExchangeName { get; set; }
    public string? QueueName { get; set; }
    public required IReadOnlyList<string> BindingPatterns { get; set; }
    public QueueOptions Queue { get; set; } = new();
}
```

**RmqOptions (alteracao):**
```csharp
// Adicionar apenas:
public Dictionary<string, ExchangeOptions> Exchanges { get; set; } = new();
```

## Criterios de Sucesso

- `ExchangeOptions` possui defaults corretos (Durable=true, AutoDelete=false)
- `TopicSubscriptionOptions` possui default para `Queue` (new QueueOptions)
- `RmqOptions.Exchanges` inicializa como dicionario vazio
- Config existente de `RmqOptions` (Connection, DefaultCloudEvents, DefaultRetry, Queues) nao eh afetada
- Build compila sem warnings
- Testes unitarios passam
