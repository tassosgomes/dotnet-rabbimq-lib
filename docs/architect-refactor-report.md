# Relatorio Executivo de Refatoracao

## Contexto

Biblioteca analisada: `Rmq.CloudEvents`

Objetivo desta intervencao: elevar a robustez operacional sem ruptura desnecessaria de API, considerando que a biblioteca ja esta em uso.

Branch de trabalho: `refactor/operational-hardening`

## Diagnostico Executivo

### Pontos fortes

- Separacao estrutural adequada entre conexao, topologia, publish, consume e envelope CloudEvents.
- Superficie de uso simples para integracao com DI em aplicacoes .NET.
- Boa cobertura unitaria para o estagio atual do projeto.
- Encapsulamento util de quorum queues e DLQ, reduzindo boilerplate do consumidor.

### Fragilidades identificadas

- Resolucao de handlers no root scope, convertendo instancias transitivas em objetos de longa duracao.
- Semantica inconsistente de retry entre publisher e consumer.
- Opcoes publicas expostas sem efeito real em runtime.
- Ausencia de backpressure configuravel no consumo.
- Garantias de entrega ainda insuficientes para cenarios criticos de publish.
- Desalinhamento entre documentacao publica e comportamento efetivo em pontos sensiveis.

## Correcoes Implementadas Nesta Branch

### 1. Isolamento correto do ciclo de vida dos handlers

Problema anterior:
o handler era resolvido uma unica vez durante a criacao do hosted service. Isso era tecnicamente insustentavel para qualquer handler com dependencias `Scoped`, estado interno ou recursos de curta duracao.

Correcao aplicada:
- o registro DI agora inclui o tipo concreto do handler;
- a execucao da mensagem cria um escopo dedicado por entrega;
- o handler correto e resolvido dentro desse escopo.

Resultado:
- compatibilidade com servicos `Scoped`;
- eliminacao da captura indevida do handler no root provider;
- reducao do risco de vazamento de estado entre mensagens.

### 2. Coerencia contratual de retry

Problema anterior:
`RetryOptions.MaxAttempts` significava total de tentativas no consumer e quantidade de retries no publisher. A mesma configuracao possuia semantica dupla, o que e um erro de contrato.

Correcao aplicada:
- publisher e consumer agora tratam `MaxAttempts` como numero total de tentativas.

Resultado:
- previsibilidade;
- documentacao e comportamento alinhados;
- menor risco de configuracoes com efeito oculto.

### 3. Backpressure configuravel no consumo

Problema anterior:
os consumers nao aplicavam `BasicQos`, omitindo controle broker-side sobre quantidade de mensagens em voo por consumer.

Correcao aplicada:
- adicionado `QueueOptions.PrefetchCount`;
- consumidores direto e topic aplicam `BasicQos` quando o valor e maior que zero.

Resultado:
- melhor controle de carga;
- menor risco de explosao de memoria e latencia sob burst.

### 4. Correcoes de fidelidade da API publica

Problema anterior:
- `DlqOptions.Enabled` nao influenciava topologia;
- `CloudEventsOptions.SpecVersion` era ignorado.

Correcao aplicada:
- a declaracao da DLQ agora respeita `DlqOptions.Enabled`;
- `SpecVersion` agora e validado em runtime.

Resultado:
- reducao de API enganosa;
- falha rapida diante de configuracao invalida;
- comportamento real aderente ao contrato exposto.

## Plano de Refatoracao Recomendado

### Fase 1. Estabilizacao imediata

Status: concluida nesta branch em pontos criticos.

- corrigir escopo de handlers por mensagem;
- unificar semantica de retry;
- introduzir `PrefetchCount`;
- fazer `DlqOptions.Enabled` e `SpecVersion` governarem comportamento real;
- reforcar testes unitarios.

### Fase 2. Garantias de entrega

Status: parcialmente concluida nesta branch.

- implementar publisher confirms;
- tratar mensagens retornadas por publish obrigatorio em exchange topic;
- explicitar no contrato quando o publish e apenas "fire-and-forget" e quando possui confirmacao efetiva do broker.

Justificativa:
sem isso, a biblioteca ainda nao fornece garantia de entrega publicadora compativel com ambientes criticos.

Entrega realizada nesta branch:
- canal de publisher agora e criado com confirmacoes do broker habilitadas;
- publish aguarda `ack` ou `nack`;
- mensagens sem rota agora falham explicitamente por `basic.return`;
- topic publish passou a usar `mandatory: true`.

Pendencia remanescente:
- avaliar ajuste fino do valor default de timeout por perfil operacional.

### Fase 3. Observabilidade e operacao

Status: pendente.

- propagar correlation id e metadados de tracing de forma padronizada;
- adicionar metricas para tentativas, nack, dlq, publish falho e latencia;
- documentar eventos operacionais e cenarios de falha.

### Fase 4. Consolidacao de contrato publico

Status: pendente.

- revisar README e amostras para refletir com rigor a configuracao real;
- documentar sem ambiguidade o significado de cada opcao;
- formalizar garantias e nao-garantias da biblioteca.

## Riscos Residuais

- testes de integracao continuam dependentes de ambiente Docker funcional.
- o valor default de timeout de confirmacao pode exigir calibracao por ambiente e latencia de broker.

## Validacao Executada

- Testes unitarios executados com sucesso:
  `dotnet test tests/Rmq.CloudEvents.Tests/Rmq.CloudEvents.Tests.csproj`

- Testes de integracao iniciados, mas sem conclusao observavel nesta sessao:
  `dotnet test tests/Rmq.CloudEvents.IntegrationTests/Rmq.CloudEvents.IntegrationTests.csproj`

## Recomendacao Final

Esta branch elimina as falhas arquiteturais mais comprometedoras sem impor ruptura gratuita de API. Nao e o fim do endurecimento necessario, mas e o ponto minimo aceitavel para continuar evoluindo a biblioteca com responsabilidade profissional.
