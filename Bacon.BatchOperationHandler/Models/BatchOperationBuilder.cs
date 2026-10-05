using Microsoft.Extensions.DependencyInjection;

namespace Bacon.BatchOperationHandler.Models;

/// <summary>
/// Batch operation builder class
/// </summary>
public sealed class BatchOperationBuilder(IServiceCollection services)
{
    /// <summary>
    /// Service collection
    /// </summary>
    internal IServiceCollection Services { get; } = services;
}