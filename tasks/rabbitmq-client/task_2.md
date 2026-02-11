## status: done

<task_context>
<domain>engine/configuracao</domain>
<type>implementation</type>
<scope>configuration</scope>
<complexity>low</complexity>
<dependencies>task_1</dependencies>
</task_context>

# Tarefa 2.0: Modelos de Configuracao e Excecoes

## Visao Geral

Implementar todas as classes de configuracao (Options) e excecoes customizadas da biblioteca. Sao classes POCO simples, sem lógica de negócio, que servem de base para todos os componentes subsequentes.

<requirements>
- Implementar todos os modelos de configuracao em `Configuration/`:
  - `RmqConnectionOptions` (HostName, Port, UserName, Password, VirtualHost, Ssl, NetworkRecoveryInterval)
  - `RetryOptions` + enum `BackoffType` (MaxAttempts=5, InitialDelay=1s, BackoffType=Exponential, UseJitter=true)
  - `DlqOptions` (Enabled=true, QueueNameSuffix=".dlq")
  - `QueueOptions` (QuorumSize=0, DeliveryLimit=5, Retry, Dlq)
  - `CloudEventsOptions` (Source, DefaultType, SpecVersion="1.0")
  - `RmqOptions` (Connection, DefaultCloudEvents, DefaultRetry, Queues)
- Implementar todas as excecoes em `Exceptions/`:
  - `RmqCloudEventsException` (base)
  - `RmqConnectionException`
  - `RmqPublishException` (com QueueName, AttemptsExhausted)
  - `RmqConsumeException`
- Todas as classes com XML docs conforme techspec
- Testes unitários para validacao de defaults e excecoes
</requirements>

## Subtarefas

- [x] 2.1 Implementar `Configuration/RmqConnectionOptions.cs`
- [x] 2.2 Implementar `Configuration/RetryOptions.cs` e `BackoffType` enum
- [x] 2.3 Implementar `Configuration/DlqOptions.cs`
- [x] 2.4 Implementar `Configuration/QueueOptions.cs`
- [x] 2.5 Implementar `Configuration/CloudEventsOptions.cs`
- [x] 2.6 Implementar `Configuration/RmqOptions.cs`
- [x] 2.7 Implementar `Exceptions/RmqCloudEventsException.cs`, `RmqConnectionException.cs`, `RmqPublishException.cs`, `RmqConsumeException.cs`
- [x] 2.8 Testes unitários para defaults das Options e propriedades das Exceptions

## Detalhes de Implementacao

Ref: techspec secoes 9 (Modelos de Configuracao) e 10 (Excecoes Customizadas).

Todas as classes de Options devem ser `sealed class` com valores default conforme techspec. As excecoes seguem hierarquia: `RmqCloudEventsException` (base) -> especializadas.

`RmqPublishException` deve conter propriedades `QueueName` e `AttemptsExhausted` para diagnóstico.

## Critérios de Sucesso

- Todas as classes compilam sem warnings
- Valores default estao corretos conforme techspec (ex: MaxAttempts=5, InitialDelay=1s)
- Testes validam defaults e construcao das excecoes
- `dotnet build` sem erros
