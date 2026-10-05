using Bacon.BatchOperationHandler.Extensions;
using Bacon.BatchOperationHandler.Interfaces.Services;
using Bacon.BatchOperationHandler.Models;
using Bacon.BatchOperationHandler.Services;
using Bacon.BatchOperationHandler.Services.Validators;
using Bacon.BatchOperationHandler.Tests.Helpers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace Bacon.BatchOperationHandler.Tests.Extensions;

[TestFixture]
public class BatchOperationHandlerExtensionsTests
{
    private sealed class StringHandler : IBatchOperationHandler<string>
    {
        public Task ProcessItemsAsync(IReadOnlyList<string> items, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task ProcessErrorsAsync(IReadOnlyList<string> items, Exception exception, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class OtherStringHandler : IBatchOperationHandler<string>
    {
        public Task ProcessItemsAsync(IReadOnlyList<string> items, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task ProcessErrorsAsync(IReadOnlyList<string> items, Exception exception, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class IntHandler : IBatchOperationHandler<int>
    {
        public Task ProcessItemsAsync(IReadOnlyList<int> items, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task ProcessErrorsAsync(IReadOnlyList<int> items, Exception exception, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class RecordingHandler : IBatchOperationHandler<string>
    {
        private readonly List<string[]> _batches = [];

        public IReadOnlyList<string[]> Batches { get { lock (_batches) { return [.. _batches]; } } }

        public Task ProcessItemsAsync(IReadOnlyList<string> items, CancellationToken cancellationToken)
        {
            lock (_batches) { _batches.Add([.. items]); }

            return Task.CompletedTask;
        }

        public Task ProcessErrorsAsync(IReadOnlyList<string> items, Exception exception, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private static ServiceCollection NewServices()
    {
        ServiceCollection services = [];
        services.AddLogging();

        return services;
    }

    #region AddBatchOperation

    [Test]
    public void AddBatchOperation_ReturnsBuilderWrappingSameCollection()
    {
        ServiceCollection services = [];

        BatchOperationBuilder builder = services.AddBatchOperation();

        Assert.That(builder.Services, Is.SameAs(services));
    }

    [Test]
    public void AddBatchOperation_RegistersSystemTimeProvider()
    {
        ServiceCollection services = [];
        services.AddBatchOperation();

        using ServiceProvider provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<TimeProvider>(), Is.SameAs(TimeProvider.System));
    }

    [Test]
    public void AddBatchOperation_DoesNotOverrideExistingTimeProvider()
    {
        ServiceCollection services = [];
        FakeTimeProvider fake = new();
        services.AddSingleton<TimeProvider>(fake);

        services.AddBatchOperation();

        using ServiceProvider provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<TimeProvider>(), Is.SameAs(fake));
    }

    [Test]
    public void AddBatchOperation_CalledTwice_RegistersTimeProviderOnce()
    {
        ServiceCollection services = [];

        services.AddBatchOperation();
        services.AddBatchOperation();

        Assert.That(services.Count(s => s.ServiceType == typeof(TimeProvider)), Is.EqualTo(1));
    }

    #endregion AddBatchOperation

    #region AddBatchHandler - registrations

    [Test]
    public void AddBatchHandler_ReturnsSameBuilder_ForChaining()
    {
        ServiceCollection services = NewServices();
        BatchOperationBuilder builder = services.AddBatchOperation();

        BatchOperationBuilder returned = builder.AddBatchHandler<string, StringHandler>();

        Assert.That(returned, Is.SameAs(builder));
    }

    [Test]
    public void AddBatchHandler_RegistersHandlerAsSingleton_SameInstanceForConcreteAndInterface()
    {
        ServiceCollection services = NewServices();
        services.AddBatchOperation().AddBatchHandler<string, StringHandler>();

        using ServiceProvider provider = services.BuildServiceProvider();

        StringHandler concrete = provider.GetRequiredService<StringHandler>();
        IBatchOperationHandler<string> asInterface = provider.GetRequiredService<IBatchOperationHandler<string>>();

        Assert.Multiple(() =>
        {
            Assert.That(asInterface, Is.SameAs(concrete));
            Assert.That(provider.GetRequiredService<StringHandler>(), Is.SameAs(concrete));
        });
    }

    [Test]
    public void AddBatchHandler_RegistersProcessorService_SameInstanceAsHostedService()
    {
        ServiceCollection services = NewServices();
        services.AddBatchOperation().AddBatchHandler<string, StringHandler>();

        using ServiceProvider provider = services.BuildServiceProvider();

        IBatchOperationProcessorService<string> client = provider.GetRequiredService<IBatchOperationProcessorService<string>>();
        BatchOperationProcessorService<string> concrete = provider.GetRequiredService<BatchOperationProcessorService<string>>();
        IHostedService hosted = provider.GetServices<IHostedService>().Single();

        Assert.Multiple(() =>
        {
            Assert.That(client, Is.SameAs(concrete));
            Assert.That(hosted, Is.SameAs(concrete));
            Assert.That(provider.GetRequiredService<IBatchOperationProcessorService<string>>(), Is.SameAs(client));
        });
    }

    [Test]
    public void AddBatchHandler_RegistersAllServicesAsSingletons()
    {
        ServiceCollection services = NewServices();
        services.AddBatchOperation().AddBatchHandler<string, StringHandler>();

        Assert.Multiple(() =>
        {
            Assert.That(services.Single(s => s.ServiceType == typeof(StringHandler)).Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
            Assert.That(services.Single(s => s.ServiceType == typeof(IBatchOperationHandler<string>)).Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
            Assert.That(services.Single(s => s.ServiceType == typeof(BatchOperationProcessorService<string>)).Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
            Assert.That(services.Single(s => s.ServiceType == typeof(IBatchOperationProcessorService<string>)).Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
            Assert.That(services.Single(s => s.ServiceType == typeof(IHostedService)).Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
            Assert.That(services.Single(s => s.ServiceType == typeof(IValidateOptions<BatchOperationHandlerOptions<string>>)).Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
        });
    }

    [Test]
    public void AddBatchHandler_RegistersOptionsValidator()
    {
        ServiceCollection services = NewServices();
        services.AddBatchOperation().AddBatchHandler<string, StringHandler>();

        using ServiceProvider provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<IValidateOptions<BatchOperationHandlerOptions<string>>>(), Is.TypeOf<BatchOperationHandlerOptionsValidatorService<string>>());
    }

    #endregion AddBatchHandler - registrations

    #region AddBatchHandler - options

    [Test]
    public void AddBatchHandler_WithoutConfigure_UsesDefaultOptions()
    {
        ServiceCollection services = NewServices();
        services.AddBatchOperation().AddBatchHandler<string, StringHandler>();

        using ServiceProvider provider = services.BuildServiceProvider();
        BatchOperationHandlerOptions<string> options = provider.GetRequiredService<IOptions<BatchOperationHandlerOptions<string>>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.MaxItemPerBatch, Is.EqualTo(250));
            Assert.That(options.SlidingInterval, Is.EqualTo(TimeSpan.FromSeconds(2)));
            Assert.That(options.MaxQueueSize, Is.Null);
        });
    }

    [Test]
    public void AddBatchHandler_WithConfigure_AppliesOptions()
    {
        ServiceCollection services = NewServices();
        services.AddBatchOperation().AddBatchHandler<string, StringHandler>(o =>
        {
            o.MaxItemPerBatch = 12;
            o.SlidingInterval = TimeSpan.FromSeconds(9);
            o.MaxQueueSize = 5_000;
        });

        using ServiceProvider provider = services.BuildServiceProvider();
        BatchOperationHandlerOptions<string> options = provider.GetRequiredService<IOptions<BatchOperationHandlerOptions<string>>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.MaxItemPerBatch, Is.EqualTo(12));
            Assert.That(options.SlidingInterval, Is.EqualTo(TimeSpan.FromSeconds(9)));
            Assert.That(options.MaxQueueSize, Is.EqualTo(5_000));
        });
    }

    [Test]
    public void AddBatchHandler_OptionsAreIsolatedPerItemType()
    {
        ServiceCollection services = NewServices();
        services.AddBatchOperation()
            .AddBatchHandler<string, StringHandler>(o => o.MaxItemPerBatch = 5)
            .AddBatchHandler<int, IntHandler>(o => o.MaxItemPerBatch = 7);

        using ServiceProvider provider = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredService<IOptions<BatchOperationHandlerOptions<string>>>().Value.MaxItemPerBatch, Is.EqualTo(5));
            Assert.That(provider.GetRequiredService<IOptions<BatchOperationHandlerOptions<int>>>().Value.MaxItemPerBatch, Is.EqualTo(7));
        });
    }

    [Test]
    public void AddBatchHandler_InvalidOptions_FailWhenResolved()
    {
        ServiceCollection services = NewServices();
        services.AddBatchOperation().AddBatchHandler<string, StringHandler>(o => o.MaxItemPerBatch = 0);

        using ServiceProvider provider = services.BuildServiceProvider();

        OptionsValidationException? ex = Assert.Throws<OptionsValidationException>(() => _ = provider.GetRequiredService<IOptions<BatchOperationHandlerOptions<string>>>().Value);
        Assert.That(ex!.Failures, Has.Some.StartWith("MaxItemPerBatch"));
    }

    [Test]
    public void AddBatchHandler_InvalidOptions_FailHostStartup_ValidateOnStart()
    {
        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddBatchOperation().AddBatchHandler<string, StringHandler>(o => o.SlidingInterval = TimeSpan.FromMilliseconds(10));

        using IHost host = builder.Build();

        OptionsValidationException? ex = Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync());
        Assert.That(ex!.Failures, Has.Some.StartWith("SlidingInterval"));
    }

    [Test]
    public async Task AddBatchHandler_ValidOptions_HostStartsAndStops()
    {
        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddBatchOperation().AddBatchHandler<string, StringHandler>();

        using IHost host = builder.Build();

        await host.StartAsync();
        await host.StopAsync();
    }

    #endregion AddBatchHandler - options

    #region AddBatchHandler - duplicates / multiple

    [Test]
    public void AddBatchHandler_SameItemTypeTwice_Throws()
    {
        ServiceCollection services = NewServices();
        BatchOperationBuilder builder = services.AddBatchOperation().AddBatchHandler<string, StringHandler>();

        InvalidOperationException? ex = Assert.Throws<InvalidOperationException>(() => builder.AddBatchHandler<string, StringHandler>());

        Assert.That(ex!.Message, Is.EqualTo("Duplicate batch handler found. Duplicate type: String"));
    }

    [Test]
    public void AddBatchHandler_SameItemTypeDifferentHandler_Throws()
    {
        ServiceCollection services = NewServices();
        BatchOperationBuilder builder = services.AddBatchOperation().AddBatchHandler<string, StringHandler>();

        Assert.Throws<InvalidOperationException>(() => builder.AddBatchHandler<string, OtherStringHandler>());
    }

    [Test]
    public void AddBatchHandler_DuplicateAcrossSeparateBuilders_Throws()
    {
        ServiceCollection services = NewServices();
        services.AddBatchOperation().AddBatchHandler<string, StringHandler>();

        Assert.Throws<InvalidOperationException>(() => services.AddBatchOperation().AddBatchHandler<string, StringHandler>());
    }

    [Test]
    public void AddBatchHandler_DuplicateAttempt_DoesNotAddRegistrations()
    {
        ServiceCollection services = NewServices();
        BatchOperationBuilder builder = services.AddBatchOperation().AddBatchHandler<string, StringHandler>();
        int countBefore = services.Count;

        Assert.Throws<InvalidOperationException>(() => builder.AddBatchHandler<string, OtherStringHandler>());

        Assert.That(services.Count, Is.EqualTo(countBefore));
    }

    [Test]
    public void AddBatchHandler_DifferentItemTypes_AreIndependent_WithTheirOwnHostedService()
    {
        ServiceCollection services = NewServices();
        services.AddBatchOperation()
            .AddBatchHandler<string, StringHandler>()
            .AddBatchHandler<int, IntHandler>();

        using ServiceProvider provider = services.BuildServiceProvider();

        object stringClient = provider.GetRequiredService<IBatchOperationProcessorService<string>>();
        object intClient = provider.GetRequiredService<IBatchOperationProcessorService<int>>();
        List<IHostedService> hosted = [.. provider.GetServices<IHostedService>()];

        Assert.Multiple(() =>
        {
            Assert.That(stringClient, Is.Not.SameAs(intClient));
            Assert.That(hosted, Has.Count.EqualTo(2));
            Assert.That(hosted, Does.Contain(stringClient));
            Assert.That(hosted, Does.Contain(intClient));
            Assert.That(provider.GetRequiredService<IBatchOperationHandler<string>>(), Is.TypeOf<StringHandler>());
            Assert.That(provider.GetRequiredService<IBatchOperationHandler<int>>(), Is.TypeOf<IntHandler>());
        });
    }

    #endregion AddBatchHandler - duplicates / multiple

    #region End to end

    [Test]
    public async Task EndToEnd_Host_ProcessesFullBatchAndFlushesRemainderOnShutdown()
    {
        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddBatchOperation().AddBatchHandler<string, RecordingHandler>(o => o.MaxItemPerBatch = 3);

        using IHost host = builder.Build();
        RecordingHandler handler = host.Services.GetRequiredService<RecordingHandler>();
        await host.StartAsync();

        IBatchOperationProcessorService<string> client = host.Services.GetRequiredService<IBatchOperationProcessorService<string>>();
        await client.EnqueueAsync(["a", "b", "c", "d"]);

        // First full batch is flushed without waiting on the timer.
        await AsyncTestHelper.WaitUntilAsync(() => handler.Batches.Count >= 1, "first batch processed");
        Assert.That(handler.Batches[0], Is.EqualTo(new[] { "a", "b", "c" }));

        // Remaining item is drained when the host stops, well before the 2s interval.
        await host.StopAsync();

        Assert.That(handler.Batches.SelectMany(b => b), Is.EqualTo(new[] { "a", "b", "c", "d" }));
    }

    [Test]
    public async Task EndToEnd_Host_FlushesPartialBatchAfterRealInterval()
    {
        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddBatchOperation().AddBatchHandler<string, RecordingHandler>(o => o.SlidingInterval = TimeSpan.FromMilliseconds(500));

        using IHost host = builder.Build();
        RecordingHandler handler = host.Services.GetRequiredService<RecordingHandler>();
        await host.StartAsync();

        await host.Services.GetRequiredService<IBatchOperationProcessorService<string>>().EnqueueAsync("only");

        await AsyncTestHelper.WaitUntilAsync(() => handler.Batches.Count == 1, "interval flush");
        Assert.That(handler.Batches[0], Is.EqualTo(new[] { "only" }));

        await host.StopAsync();
    }

    #endregion End to end
}
