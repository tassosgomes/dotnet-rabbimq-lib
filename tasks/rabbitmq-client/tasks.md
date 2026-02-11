# Tasks - Rmq.CloudEvents Library

> Cada tarefa possui detalhamento completo em `tasks/task_N.md`

- [X] **task_1** - Scaffolding da Solution e Projetos (Directory.Build.props, .sln, .csproj, .gitignore, estrutura de pastas)
- [X] **task_2** - Modelos de Configuracao e Excecoes (RmqOptions, ConnectionOptions, RetryOptions, DlqOptions, QueueOptions, CloudEventsOptions + todas as excecoes customizadas + testes)
- [X] **task_3** - Serializacao e CloudEvents Wrapper (IMessageSerializer, SystemTextJsonMessageSerializer, CloudEventMetadata, ICloudEventWrapper, CloudEventWrapper + testes unitarios)
- [X] **task_4** - Connection Manager e Queue Manager (IRmqConnectionManager, RmqConnectionManager, IQueueManager, QueueManager + testes unitarios)
- [X] **task_5** - Publisher com Retry Exponencial (IRmqPublisher, RmqPublisher, Polly ResiliencePipeline + testes unitarios)
- [X] **task_6** - Consumer com Retry e ACK/NACK (MessageContext, IRmqMessageHandler, IRmqConsumer, RmqAsyncConsumerHandler, RmqConsumer + testes unitarios)
- [X] **task_7** - Dependency Injection e ServiceCollection Extensions (AddRmqCloudEvents, AddRmqConsumer + testes unitarios)
- [X] **task_8** - Testes de Integracao com Testcontainers (RabbitMqFixture, roundtrip, CloudEvents wire, DLQ routing, multi-queue)
- [X] **task_9** - Sample Application e CI/CD (Program.cs de exemplo, GitHub Actions workflow)
- [X] **task_10** - Validacao Final, Build e Correcoes (build Release, testes, cobertura, pack NuGet, revisao)
