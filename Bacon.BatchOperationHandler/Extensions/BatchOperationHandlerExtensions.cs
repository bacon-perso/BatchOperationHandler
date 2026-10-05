using Bacon.BatchOperationHandler.Interfaces.Services;
using Bacon.BatchOperationHandler.Models;
using Bacon.BatchOperationHandler.Services;
using Bacon.BatchOperationHandler.Services.Validators;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Bacon.BatchOperationHandler.Extensions;

/// <summary>
/// Batch operation handler extension
/// </summary>
public static class BatchOperationHandlerExtensions
{
    #region Publics

    /// <summary>
    /// Add batch operation.
    /// </summary>
    /// <param name="services">service collection</param>
    /// <returns>BatchOperationBuilder</returns>
    public static BatchOperationBuilder AddBatchOperation(this IServiceCollection services)
    {
        services.TryAddSingleton(TimeProvider.System);

        return new(services);
    }

    /// <summary>
    /// Add batch operation handler
    /// </summary>
    /// <typeparam name="TItem">Batch item class </typeparam>
    /// <typeparam name="TService">Batch item handler class implementing IBatchOperationHandler&lt;TItem&gt;. Registered as a singleton.</typeparam>
    /// <param name="batchOperationBuilder">BatchOperationBuilder</param>
    /// <param name="configureOptions">BatchOperationHandlerOptions action. Optional, defaults are used when omitted.</param>
    /// <returns>BatchOperationBuilder</returns>
    /// <exception cref="InvalidOperationException">A handler is already registered for <typeparamref name="TItem"/>.</exception>
    public static BatchOperationBuilder AddBatchHandler<TItem, TService>(this BatchOperationBuilder batchOperationBuilder, Action<BatchOperationHandlerOptions<TItem>>? configureOptions = null) where TService : class, IBatchOperationHandler<TItem>
    {
        if (batchOperationBuilder.Services.Any(a => a.ServiceType == typeof(IBatchOperationProcessorService<TItem>)))
        {
            throw new InvalidOperationException($"Duplicate batch handler found. Duplicate type: {typeof(TItem).Name}");
        }

        //Adds 1 BatchOperationHandler per TItem
        batchOperationBuilder.Services.AddSingleton<TService>().AddSingleton<IBatchOperationHandler<TItem>, TService>(s => s.GetRequiredService<TService>());

        //Adds 1 BatchOperationProcessorService per TItem
        batchOperationBuilder.Services.AddSingleton<BatchOperationProcessorService<TItem>>().AddSingleton<IBatchOperationProcessorService<TItem>, BatchOperationProcessorService<TItem>>(s => s.GetRequiredService<BatchOperationProcessorService<TItem>>());

        //Adds 1 Hosted Service per TItem
        batchOperationBuilder.Services.AddHostedService(provider => provider.GetRequiredService<BatchOperationProcessorService<TItem>>());

        OptionsBuilder<BatchOperationHandlerOptions<TItem>> optionsBuilder = batchOperationBuilder.Services.AddOptions<BatchOperationHandlerOptions<TItem>>();

        if(configureOptions is not null)
        {
            optionsBuilder.Configure(configureOptions);
        }

        batchOperationBuilder.Services.AddSingleton<IValidateOptions<BatchOperationHandlerOptions<TItem>>, BatchOperationHandlerOptionsValidatorService<TItem>>();
        optionsBuilder.ValidateOnStart();

        return batchOperationBuilder;
    }

    #endregion Publics
}