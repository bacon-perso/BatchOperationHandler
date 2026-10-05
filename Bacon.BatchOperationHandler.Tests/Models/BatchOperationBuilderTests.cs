using Bacon.BatchOperationHandler.Models;
using Microsoft.Extensions.DependencyInjection;

namespace Bacon.BatchOperationHandler.Tests.Models;

[TestFixture]
public class BatchOperationBuilderTests
{
    [Test]
    public void Constructor_ExposesProvidedServiceCollection()
    {
        ServiceCollection services = [];

        BatchOperationBuilder builder = new(services);

        Assert.That(builder.Services, Is.SameAs(services));
    }
}
