using Bacon.BatchOperationHandler.Interfaces.Services;

namespace Bacon.BatchOperationHandler.Samples.Services;

public class AnotherWeatherForcastBatchHandlerService : IBatchOperationHandler<AnotherWeatherForecast>
{
    public Task ProcessItemsAsync(IReadOnlyList<AnotherWeatherForecast> items, CancellationToken cancellationToken)
    {
        Console.WriteLine($"AnotherWeatherForcastBatchHandlerService processed {items.Count} items");

        return Task.CompletedTask;
    }

    public Task ProcessErrorsAsync(IReadOnlyList<AnotherWeatherForecast> items, Exception exception, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
