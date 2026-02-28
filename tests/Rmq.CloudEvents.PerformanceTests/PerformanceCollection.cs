using Xunit;

namespace Rmq.CloudEvents.PerformanceTests;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class PerformanceCollection : ICollectionFixture<RabbitMqPerformanceFixture>
{
    public const string Name = "PerformanceCollection";
}
