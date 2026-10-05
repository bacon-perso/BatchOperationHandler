using Bacon.BatchOperationHandler.Interfaces.Services;
using Bacon.BatchOperationHandler.Samples.Models;
using System.Security.Cryptography;

namespace Bacon.BatchOperationHandler.Samples.Services;


public class WeatherForcastBatchHandlerService() : IBatchOperationHandler<WeatherForecast>
{
    public Task ProcessItemsAsync(IReadOnlyList<WeatherForecast> items, CancellationToken cancellationToken)
    {
        int random = RandomNumberGenerator.GetInt32(0, 11);

        switch(random)
        {
            case 0: throw new InvalidCastException("InvalidCast");

            case 1: throw new InvalidOperationException("InvalidOperation");

            case 2: throw new InvalidDataException("InvalidDataException");
        }

        Console.WriteLine($"WeatherForcastBatchHandlerService processed {items.Count} items");

        return Task.CompletedTask;
    }

    public Task ProcessErrorsAsync(IReadOnlyList<WeatherForecast> items, Exception exception, CancellationToken cancellationToken)
    {
        //Could requeue items

        Console.WriteLine($"Process failed with batch of {items.Count} items.Exception of type '{exception.Message}'");

        return Task.CompletedTask;
    }
}