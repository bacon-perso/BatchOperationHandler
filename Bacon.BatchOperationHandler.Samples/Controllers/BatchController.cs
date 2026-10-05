using Bacon.BatchOperationHandler.Interfaces.Services;
using Bacon.BatchOperationHandler.Samples.Models;
using Microsoft.AspNetCore.Mvc;

namespace Bacon.BatchOperationHandler.Samples.Controllers;

[ApiController]
[Route("batch")]
public class BatchController(IBatchOperationProcessorService<WeatherForecast> weatherForecastbatchOperationProcessorService, IBatchOperationProcessorService<AnotherWeatherForecast> anotherWeatherForecastbatchOperationProcessorService) : ControllerBase
{
    private static readonly string[] Summaries =
    [
        "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
    ];

    [HttpPost]
    [Route("weather")]
    [ProducesResponseType(StatusCodes.Status202Accepted, Type = typeof(void))]
    public async Task CreateWeatherForecast(CancellationToken cancellationToken)
    {
        _ = Task.Run(async () =>
        {
            List<WeatherForecast> weatherForecasts = [];
            for (int idx = 0; idx < 175; ++idx)
            {
                weatherForecasts.Add(new()
                {
                    Date = DateOnly.FromDateTime(DateTime.Now.AddDays(1)),
                    TemperatureC = Random.Shared.Next(-20, 55),
                    Summary = Summaries[Random.Shared.Next(Summaries.Length)]
                });
            }

            await weatherForecastbatchOperationProcessorService.EnqueueAsync(weatherForecasts, cancellationToken);
        });
    }

    [HttpPost]
    [Route("another")]
    [ProducesResponseType(StatusCodes.Status202Accepted, Type = typeof(void))]
    public async Task CreateAnotherWeatherForecast(CancellationToken cancellationToken)
    {
        _ = Task.Run(async () =>
        {
            for (int idx = 0; idx < 15; ++idx)
            {
                AnotherWeatherForecast anotherWeatherForecast = new()
                {
                    Date = DateOnly.FromDateTime(DateTime.Now.AddDays(1)),
                    TemperatureC = Random.Shared.Next(-20, 55),
                    Summary = Summaries[Random.Shared.Next(Summaries.Length)]
                };

                Console.WriteLine($"Enqueued item at {DateTime.Now.ToString("o")}");
                await anotherWeatherForecastbatchOperationProcessorService.EnqueueAsync(anotherWeatherForecast, cancellationToken);

                await Task.Delay(1500, cancellationToken);
            }
        });
    }
}
