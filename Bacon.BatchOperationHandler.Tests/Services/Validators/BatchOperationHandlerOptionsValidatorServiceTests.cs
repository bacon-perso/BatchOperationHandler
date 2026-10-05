using Bacon.BatchOperationHandler.Models;
using Bacon.BatchOperationHandler.Services.Validators;
using Microsoft.Extensions.Options;

namespace Bacon.BatchOperationHandler.Tests.Services.Validators;

[TestFixture]
public class BatchOperationHandlerOptionsValidatorServiceTests
{
    private BatchOperationHandlerOptionsValidatorService<string> _validator = null!;

    [SetUp]
    public void SetUp() => _validator = new();

    private ValidateOptionsResult Validate(ushort maxItems = 250, TimeSpan? interval = null, int? maxQueueSize = null, string? name = null)
    {
        return _validator.Validate(name, new BatchOperationHandlerOptions<string>
        {
            MaxItemPerBatch = maxItems,
            SlidingInterval = interval ?? TimeSpan.FromSeconds(2),
            MaxQueueSize = maxQueueSize
        });
    }

    #region Defaults

    [Test]
    public void Validate_DefaultOptions_Succeeds()
    {
        ValidateOptionsResult result = _validator.Validate(null, new BatchOperationHandlerOptions<string>());

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.True);
            Assert.That(result.Failed, Is.False);
            Assert.That(result, Is.SameAs(ValidateOptionsResult.Success));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("named")]
    public void Validate_OptionsName_DoesNotAffectResult(string? name)
    {
        Assert.That(Validate(name: name).Succeeded, Is.True);
        Assert.That(Validate(maxItems: 0, name: name).Failed, Is.True);
    }

    #endregion Defaults

    #region MaxItemPerBatch

    [TestCase((ushort)1, true)]
    [TestCase((ushort)2, true)]
    [TestCase((ushort)250, true)]
    [TestCase((ushort)999, true)]
    [TestCase((ushort)1000, true)]
    [TestCase((ushort)0, false)]
    [TestCase((ushort)1001, false)]
    [TestCase(ushort.MaxValue, false)]
    public void Validate_MaxItemPerBatch_Boundaries(ushort value, bool expectedValid)
    {
        ValidateOptionsResult result = Validate(maxItems: value);

        Assert.That(result.Succeeded, Is.EqualTo(expectedValid));
    }

    [Test]
    public void Validate_MaxItemPerBatchOutOfRange_ReportsSingleDescriptiveError()
    {
        ValidateOptionsResult result = Validate(maxItems: 0);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failures, Has.Exactly(1).Items);
            Assert.That(result.Failures!.Single(), Is.EqualTo("MaxItemPerBatch must be between 1 and 1000."));
        });
    }

    #endregion MaxItemPerBatch

    #region SlidingInterval

    [TestCase(500, true)]
    [TestCase(501, true)]
    [TestCase(2000, true)]
    [TestCase(299_999, true)]
    [TestCase(300_000, true)]
    [TestCase(499, false)]
    [TestCase(1, false)]
    [TestCase(0, false)]
    [TestCase(-1, false)]
    [TestCase(-2000, false)]
    [TestCase(300_001, false)]
    [TestCase(3_600_000, false)]
    public void Validate_SlidingInterval_Boundaries(int milliseconds, bool expectedValid)
    {
        ValidateOptionsResult result = Validate(interval: TimeSpan.FromMilliseconds(milliseconds));

        Assert.That(result.Succeeded, Is.EqualTo(expectedValid));
    }

    [Test]
    public void Validate_SlidingIntervalExtremes_Fail()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Validate(interval: TimeSpan.MinValue).Failed, Is.True);
            Assert.That(Validate(interval: TimeSpan.MaxValue).Failed, Is.True);
            Assert.That(Validate(interval: TimeSpan.Zero).Failed, Is.True);
            Assert.That(Validate(interval: Timeout.InfiniteTimeSpan).Failed, Is.True);
        });
    }

    [Test]
    public void Validate_SlidingIntervalOutOfRange_ReportsSingleDescriptiveError()
    {
        ValidateOptionsResult result = Validate(interval: TimeSpan.FromMilliseconds(100));

        Assert.Multiple(() =>
        {
            Assert.That(result.Failures, Has.Exactly(1).Items);
            Assert.That(result.Failures!.Single(), Is.EqualTo("SlidingInterval must be between 500 milliseconds and 300 seconds."));
        });
    }

    #endregion SlidingInterval

    #region MaxQueueSize

    [Test]
    public void Validate_MaxQueueSizeNull_IsUnbounded_AndValid()
    {
        Assert.That(Validate(maxQueueSize: null).Succeeded, Is.True);
    }

    [TestCase(250, 250, true)]  // equals MaxItemPerBatch
    [TestCase(250, 251, true)]
    [TestCase(250, 100_000, true)]  // upper bound inclusive
    [TestCase(250, 249, false)]  // below MaxItemPerBatch
    [TestCase(250, 100_001, false)]  // above upper bound
    [TestCase(1, 1, true)]
    [TestCase(1, 0, false)]
    [TestCase(1, -1, false)]
    [TestCase(1000, 1000, true)]
    [TestCase(1000, 999, false)]
    [TestCase(1, int.MaxValue, false)]
    [TestCase(1, int.MinValue, false)]
    public void Validate_MaxQueueSize_Boundaries(int maxItems, int maxQueueSize, bool expectedValid)
    {
        ValidateOptionsResult result = Validate(maxItems: (ushort)maxItems, maxQueueSize: maxQueueSize);

        Assert.That(result.Succeeded, Is.EqualTo(expectedValid));
    }

    [Test]
    public void Validate_MaxQueueSizeOutOfRange_ReportsSingleDescriptiveError()
    {
        ValidateOptionsResult result = Validate(maxQueueSize: 10);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failures, Has.Exactly(1).Items);
            Assert.That(result.Failures!.Single(), Is.EqualTo("MaxQueueSize must be between MaxItemPerBatch and 100000."));
        });
    }

    #endregion MaxQueueSize

    #region Combined

    [Test]
    public void Validate_AllPropertiesInvalid_ReportsAllErrors()
    {
        ValidateOptionsResult result = Validate(maxItems: 0, interval: TimeSpan.Zero, maxQueueSize: 200_000);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.Failures, Has.Exactly(3).Items);
            Assert.That(result.Failures, Has.Some.StartWith("MaxItemPerBatch"));
            Assert.That(result.Failures, Has.Some.StartWith("SlidingInterval"));
            Assert.That(result.Failures, Has.Some.StartWith("MaxQueueSize"));
        });
    }

    [Test]
    public void Validate_TwoInvalid_ReportsExactlyTwoErrors()
    {
        ValidateOptionsResult result = Validate(maxItems: 2000, interval: TimeSpan.FromHours(1));

        Assert.That(result.Failures, Has.Exactly(2).Items);
    }

    [Test]
    public void Validate_MaxItemInvalidAndQueueSizeBelowIt_ReportsBoth()
    {
        // 2000 > 1000 is invalid; a queue of 1500 is below MaxItemPerBatch, so also invalid.
        ValidateOptionsResult result = Validate(maxItems: 2000, maxQueueSize: 1500);

        Assert.That(result.Failures, Has.Exactly(2).Items);
    }

    [Test]
    public void Validate_AllBoundariesAtOnce_Succeeds()
    {
        ValidateOptionsResult result = Validate(maxItems: 1000, interval: TimeSpan.FromMinutes(5), maxQueueSize: 100_000);

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_ImplementsIValidateOptions()
    {
        Assert.That(_validator, Is.InstanceOf<IValidateOptions<BatchOperationHandlerOptions<string>>>());
    }

    #endregion Combined
}
