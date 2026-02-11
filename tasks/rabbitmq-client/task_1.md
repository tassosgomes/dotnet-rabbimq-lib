## status: pending

<task_context>
<domain>infra/projeto</domain>
<type>implementation</type>
<scope>configuration</scope>
<complexity>low</complexity>
<dependencies>dotnet SDK 8.0</dependencies>
</task_context>

# Tarefa 1.0: Scaffolding da Solution e Projetos

## Visao Geral

Criar toda a estrutura de diretórios, solution, projetos .csproj e arquivos de configuracao base. Ao final desta tarefa o `dotnet build` deve compilar com sucesso (sem código de negócio, apenas estrutura).

<requirements>
- Criar `Directory.Build.props` com net8.0, LangVersion latest, Nullable enable, TreatWarningsAsErrors true
- Criar solution `Rmq.CloudEvents.sln`
- Criar projeto principal `src/Rmq.CloudEvents/Rmq.CloudEvents.csproj` com todas as dependencias do techspec (RabbitMQ.Client 7.*, Polly.Core 8.*, CloudNative.CloudEvents 2.*, CloudNative.CloudEvents.SystemTextJson 2.*, Microsoft.Extensions.Logging.Abstractions 8.*, Microsoft.Extensions.DependencyInjection.Abstractions 8.*, Microsoft.Extensions.Options 8.*)
- Criar projeto de testes unitários `tests/Rmq.CloudEvents.Tests/Rmq.CloudEvents.Tests.csproj` com xUnit, Moq, FluentAssertions e referencia ao projeto principal
- Criar projeto de testes de integração `tests/Rmq.CloudEvents.IntegrationTests/Rmq.CloudEvents.IntegrationTests.csproj` com Testcontainers.RabbitMq e referencia ao projeto principal
- Criar projeto sample `samples/Rmq.CloudEvents.Sample/Rmq.CloudEvents.Sample.csproj`
- Atualizar `.gitignore` para .NET (bin/, obj/, artifacts/, *.user, etc.)
- Criar estrutura de pastas vazias dentro de `src/Rmq.CloudEvents/`: Configuration/, Connection/, Infrastructure/, CloudEvents/, Publishing/, Consuming/, Serialization/, Exceptions/, Extensions/
</requirements>

## Subtarefas

- [x] 1.1 Criar `Directory.Build.props` na raiz
- [x] 1.2 Criar `Rmq.CloudEvents.sln` e adicionar todos os projetos
- [x] 1.3 Criar `src/Rmq.CloudEvents/Rmq.CloudEvents.csproj` com dependencias
- [x] 1.4 Criar `tests/Rmq.CloudEvents.Tests/Rmq.CloudEvents.Tests.csproj`
- [x] 1.5 Criar `tests/Rmq.CloudEvents.IntegrationTests/Rmq.CloudEvents.IntegrationTests.csproj`
- [x] 1.6 Criar `samples/Rmq.CloudEvents.Sample/Rmq.CloudEvents.Sample.csproj`
- [x] 1.7 Atualizar `.gitignore`
- [x] 1.8 Validar com `dotnet build` e `dotnet restore`

## Detalhes de Implementacao

Ref: techspec secoes 4 (Estrutura da Solucao) e 5 (Configuracao do Projeto).

Estrutura alvo:
```
dotnet-rabbimq-lib/
├── Directory.Build.props
├── Rmq.CloudEvents.sln
├── src/Rmq.CloudEvents/
│   ├── Rmq.CloudEvents.csproj
│   ├── Configuration/
│   ├── Connection/
│   ├── Infrastructure/
│   ├── CloudEvents/
│   ├── Publishing/
│   ├── Consuming/
│   ├── Serialization/
│   ├── Exceptions/
│   └── Extensions/
├── tests/
│   ├── Rmq.CloudEvents.Tests/
│   └── Rmq.CloudEvents.IntegrationTests/
└── samples/Rmq.CloudEvents.Sample/
```

## Critérios de Sucesso

- `dotnet restore` executa sem erros
- `dotnet build` compila com sucesso
- Todos os projetos estao referenciados na solution
- Estrutura de pastas conforme techspec
