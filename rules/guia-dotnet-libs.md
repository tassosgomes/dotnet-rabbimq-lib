# Guia de Melhores Práticas para Criação de Bibliotecas Profissionais em .NET

## 1. Objetivo de uma biblioteca profissional

Uma biblioteca .NET profissional, segundo as diretrizes oficiais da Microsoft, deve ser:

- **Inclusiva**: rodar em vários tipos de apps/plataformas.
- **Estável**: conviver bem com outras bibliotecas no mesmo processo.
- **Projetada para evoluir**: permitir melhorias sem quebrar quem já usa.
- **Depurável**: fácil de diagnosticar problemas.
- **Confiável**: publicada e mantida seguindo boas práticas de segurança e qualidade.

Tenha isso como “norte” ao tomar decisões de design, empacotamento e publicação.

---

## 2. Decisões iniciais de projeto

### 2.1. Tipo de projeto e template

Use o template moderno de class library:

```bash
dotnet new classlib -n MinhaEmpresa.MinhaLib
```

- Prefira **SDK-style projects** (padrão no .NET Core/5+/6+/8+).
- Use a **versão mais recente de C#** possível compatível com seus consumidores.

### 2.2. Target frameworks

Para bibliotecas amplamente reutilizáveis, a recomendação é que sejam o mais inclusivas possível.

Padrão comum hoje:

- Se sua lib é **novo desenvolvimento** e você não tem exigência de .NET Framework:
  - `net8.0` (ou LTS atual) como alvo principal.
- Se precisa ser usada por aplicações mais antigas / variedade de runtimes:
  - **multi-target** com algo como:
    - `netstandard2.0` (compatível com .NET Framework 4.6.1+ e vários runtimes)
    - e um alvo moderno, ex.: `net8.0` para recursos mais novos e melhor performance.

Exemplo no `.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFrameworks>netstandard2.0;net8.0</TargetFrameworks>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
  </PropertyGroup>

</Project>
```

---

## 3. Estrutura da solução

Organize a solução de forma “enterprise”:

- `src/MinhaEmpresa.MinhaLib/MinhaEmpresa.MinhaLib.csproj`
- `tests/MinhaEmpresa.MinhaLib.Tests/MinhaEmpresa.MinhaLib.Tests.csproj`
- (Opcional) `samples/MinhaEmpresa.MinhaLib.Samples/…`

Benefícios:

- Separação clara entre código de produção, testes e exemplos.
- Facilita CI/CD, empacotamento e navegação.

---

## 4. Configuração básica de qualidade

No `.csproj` da biblioteca, configure:

```xml
<PropertyGroup>
  <Nullable>enable</Nullable>           <!-- Referências anuláveis ativadas -->
  <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
  <ImplicitUsings>enable</ImplicitUsings>
</PropertyGroup>
```

- **Nullable Reference Types**: melhora a segurança contra `NullReferenceException`.
- **Warnings como erros**: evita regressões de qualidade silenciosas.
- Ative **analyzers** (já vêm integrados nas versões recentes do SDK) e, se quiser ser mais rígido:
  - StyleCop Analyzers, Roslyn Analyzers específicos etc.

---

## 5. Design da API pública

As *Framework Design Guidelines* da Microsoft são a referência canônica para design de APIs .NET. Abaixo um resumo dos pontos principais.

### 5.1. Comece pelos cenários (Scenario-Driven Design)

- Liste os **cenários principais** de uso da biblioteca.
- Escreva o código ideal que o consumidor escreveria para cada cenário.
- “Molde” a API para que esses cenários fiquem simples e claros.

Isso ajuda a criar uma API:

- Intuitiva (autoexplicativa).
- Com poucos passos para os casos mais comuns.
- Focada nas necessidades reais dos usuários.

### 5.2. Nomes e organização

Siga as convenções oficiais de naming e organização.

- **Namespaces**: `MinhaEmpresa.MinhaArea.MinhaLib`.
  - Comece por empresa/organização, depois o “domínio”.
- **Classes públicas**: PascalCase (`MyService`, `OrderProcessor`).
- **Métodos**: PascalCase (`Create`, `GetById`, `TryParse`).
- **Propriedades**: PascalCase (`Count`, `IsEnabled`).
- **Parâmetros e variáveis locais**: camelCase (`orderId`, `options`).
- Nomes **curtos, mas descritivos**; evite abreviações obscuras.

### 5.3. Que tipos expor?

- Use **classes** para tipos de comportamento/estado complexos.
- Use **structs** apenas se:
  - forem pequenos e imutáveis;
  - representarem valores (tipo primitivo de domínio: `Point`, `Money`);
  - e forem usados com frequência em coleções ou alto volume.
- Use **enums** para conjuntos fechados de opções.
- Exponha **interfaces** para permitir substituição/extensibilidade (`ILogger`, `IRepository`).
- Considere **records** para DTOs imutáveis, quando fizer sentido.

### 5.4. Métodos, assinaturas e async

- Prefira APIs **assíncronas** para I/O, usando o sufixo `Async` (`GetUserAsync`).
- Use `CancellationToken` em métodos async com operações potencialmente longas.
- Evite métodos com **muitos parâmetros**; agrupe em um objeto de opções.
- Use tipos .NET conhecidos e bem suportados:
  - `DateTimeOffset` em vez de `DateTime` quando for data/hora com fuso.
  - `IReadOnlyCollection<T>` para coleções somente leitura.
  - `IEnumerable<T>` para sequências iteráveis.

### 5.5. Exceções e erros

- Use **exceções**, não códigos de erro de retorno.
- Valide argumentos e jogue:
  - `ArgumentNullException`, `ArgumentException`, `ArgumentOutOfRangeException` etc.
- Não esconda exceções irrecuperáveis.
- Crie exceções customizadas apenas quando:
  - houver um cenário real de tratamento específico; e
  - faça-as derivar de `Exception` (não de `ApplicationException`).

Mantenha mensagens de erro:

- Claras, com contexto mínimo para diagnóstico (mas sem vazar dados sensíveis).

### 5.6. Imutabilidade e thread-safety

- Prefira **tipos imutáveis** sempre que possível (especialmente para DTOs/valores).
- Documente a **thread-safety** da API:
  - “Instâncias de `X` são thread-safe para leitura concorrente, mas não para escrita concorrente”, etc.
- Evite estados globais e singletons “escondidos” dentro da lib.

### 5.7. Superfície pública mínima

- “Feche” o máximo que puder:
  - `internal` em vez de `public` quando algo não é para ser usado externamente.
- Expor menos coisas facilita:
  - manter compatibilidade;
  - evoluir a implementação por dentro;
  - reduzir confusão do usuário.

Ferramentas como PublicApiAnalyzer ou testes de “approval” ajudam a garantir que a API pública não muda sem intenção.

---

## 6. Qualidade interna: testes, análise estática e CI

### 6.1. Testes

- Tenha **testes unitários** cobrindo comportamento principal.
- Tenha **testes de integração** quando sua lib conversa com serviços externos.
- Crie testes de **regressão** para bugs corrigidos.

### 6.2. Analyzers e estilo

- Habilite **.NET analyzers** (já vêm com o SDK moderno).
- Considere:
  - **StyleCop.Analyzers** para consistência de estilo;
  - Regras de naming, documentação XML, etc.

### 6.3. CI/CD

- Use uma pipeline (GitHub Actions, Azure DevOps, GitLab CI…) que:
  - Roda testes em cada PR;
  - Verifica qualidade (analyzers, coverage mínima);
  - Gera pacote `.nupkg` somente em builds de release “oficiais”.

---

## 7. Empacotamento e publicação (NuGet)

### 7.1. Sempre empacote como NuGet

Recomenda-se **distribuir bibliotecas via NuGet**.

No `.csproj` principal da lib:

```xml
<PropertyGroup>
  <PackageId>MinhaEmpresa.MinhaLib</PackageId>
  <Version>1.0.0</Version>
  <Authors>Minha Empresa</Authors>
  <Company>Minha Empresa</Company>
  <Description>Descrição clara e objetiva da biblioteca.</Description>
  <PackageTags>logging;rest;cliente-api</PackageTags>
  <RepositoryUrl>https://github.com/minha-empresa/minha-lib</RepositoryUrl>
  <PackageProjectUrl>https://github.com/minha-empresa/minha-lib</PackageProjectUrl>
  <PackageLicenseExpression>MIT</PackageLicenseExpression>
  <IncludeSymbols>true</IncludeSymbols>
  <SymbolPackageFormat>snupkg</SymbolPackageFormat>
  <PublishRepositoryUrl>true</PublishRepositoryUrl>
</PropertyGroup>
```

Para criar o pacote:

```bash
dotnet pack -c Release
```

Para publicar (exemplo com nuget.org):

```bash
dotnet nuget push bin/Release/MinhaEmpresa.MinhaLib.1.0.0.nupkg \
  --api-key <API_KEY> \
  --source https://api.nuget.org/v3/index.json
```

### 7.2. Versão e SemVer

Use **SemVer 2.0.0** para versionamento de pacotes NuGet.

- `MAJOR.MINOR.PATCH[-pre]`
  - **MAJOR**: quebra de compatibilidade de API.
  - **MINOR**: novas funcionalidades compatíveis.
  - **PATCH**: correções de bug sem mudar API.

Não publique versões que quebram a API pública sem **subir o MAJOR**.

### 7.3. Dependências

Boas práticas de autoria de pacote NuGet:

- Mantenha o número de dependências o mais **baixo possível**.
- Evite fixar **versão exata** de dependências (ex.: `1.2.3`); prefira intervalos compatíveis ou versão mínima.
- Não reempacote como dependência coisas triviais que poderiam ser implementadas em poucas linhas (para evitar “árvore de dependências” gigante).
- Evite publicar variantes estranhas do mesmo pacote (`MinhaLib.StrongNamed`, `MinhaLib.NoDependencies`, etc.).

### 7.4. SourceLink, símbolos e debugging

Boas bibliotecas são fáceis de debugar.

- Habilite **SourceLink** para permitir debug no código-fonte diretamente a partir do pacote.
- Publique **símbolos** (`.snupkg`) para melhorar a experiência de debug.

---

## 8. Documentação e exemplo de uso

### 8.1. Comentários XML

- Adicione **comentários XML** em todas as APIs públicas.
- Gere documentação para IntelliSense – isso é crucial para adoção.

### 8.2. README, CHANGELOG, exemplos

No repositório (e referenciando no NuGet):

- `README.md`:
  - O que a biblioteca faz.
  - Exemplos de uso básicos e avançados.
  - Requisitos (versões de .NET).
- `CHANGELOG.md`:
  - Histórico de versões e mudanças.
- `CONTRIBUTING.md` (se open source):
  - Como contribuir, abrir issues, padrões de código.
- **Projeto de samples**:
  - Pequenos exemplos executáveis demonstrando cenários-chave.

---

## 9. Segurança e confiança

- Use **2FA** na conta do nuget.org.
- Idealmente, assine os pacotes com chave de assinatura (Package Signing).
- Mantenha as dependências atualizadas e verifique vulnerabilidades (GitHub Dependabot, etc.).
- Evite logar dados sensíveis em exceções ou mensagens de log.

---

## 10. Planejando evolução sem quebrar consumidores

Para que a biblioteca seja “designed to evolve”:

- Pense na API pública como **contrato**:
  - Evite mudar ou remover membros públicos sem necessidade.
  - Em vez de remover, às vezes é melhor:
    - Marcar como `[Obsolete]` com mensagem e plano de remoção futura.
- Quando precisar quebrar API:
  - Planeje uma versão MAJOR nova.
  - Documente breaking changes no CHANGELOG e README.
- Adicione novos recursos de forma incremental e compatível sempre que possível.

---

## 11. Fluxo típico profissional (resumo passo a passo)

1. **Planejar** cenários principais de uso.
2. **Criar solução**:
   - `src/MinhaEmpresa.MinhaLib`
   - `tests/MinhaEmpresa.MinhaLib.Tests`
   - (opcional) `samples/…`
3. **Configurar `.csproj`**:
   - Multi-target (`netstandard2.0;net8.0` se necessário).
   - `Nullable=enable`, warnings como erros, metadata de pacote.
4. **Desenhar API pública**:
   - Seguindo boas práticas de naming, tipos, exceptions.
5. **Implementar com testes**:
   - Testes unitários, integração, uso de analyzers.
6. **Configurar CI**:
   - Build + testes em cada PR.
   - `dotnet pack` em builds de release.
7. **Publicar no NuGet**:
   - Seguindo boas práticas de autoria de pacote (SemVer, metadata rica, poucas dependências).
8. **Documentar e manter**:
   - README, CHANGELOG, exemplos, issues/PRs, releases.
