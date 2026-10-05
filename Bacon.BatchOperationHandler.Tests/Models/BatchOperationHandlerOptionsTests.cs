using Bacon.BatchOperationHandler.Models;

namespace Bacon.BatchOperationHandler.Tests.Models;

[TestFixture]
public class BatchOperationHandlerOptionsTests
{
    [Test]
    public void Defaults_AreDocumentedValues()
    {
        BatchOperationHandlerOptions<string> options = new();

        Assert.Multiple(() =>
        {
            Assert.That(options.MaxItemPerBatch, Is.EqualTo(250));
            Assert.That(options.SlidingInterval, Is.EqualTo(TimeSpan.FromSeconds(2)));
            Assert.That(options.MaxQueueSize, Is.Null);
        });
    }

    [Test]
    public void Properties_AreSettable()
    {
        BatchOperationHandlerOptions<string> options = new()
        {
            MaxItemPerBatch = 10,
            SlidingInterval = TimeSpan.FromSeconds(7),
            MaxQueueSize = 99
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.MaxItemPerBatch, Is.EqualTo(10));
            Assert.That(options.SlidingInterval, Is.EqualTo(TimeSpan.FromSeconds(7)));
            Assert.That(options.MaxQueueSize, Is.EqualTo(99));
        });
    }

    [Test]
    public void Instances_AreIndependent()
    {
        BatchOperationHandlerOptions<string> first = new() { MaxItemPerBatch = 5 };
        BatchOperationHandlerOptions<string> second = new();

        Assert.That(second.MaxItemPerBatch, Is.EqualTo(250));
        Assert.That(first, Is.Not.SameAs(second));
    }
}
