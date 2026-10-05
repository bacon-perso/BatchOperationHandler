using Bacon.BatchOperationHandler.Extensions;
using Bacon.BatchOperationHandler.Samples;
using Bacon.BatchOperationHandler.Samples.Models;
using Bacon.BatchOperationHandler.Samples.Services;

WebApplicationBuilder webApplicationBuilder = WebApplication.CreateBuilder(args);

// Add services to the container.

webApplicationBuilder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
webApplicationBuilder.Services.AddOpenApi();

webApplicationBuilder.Services.AddBatchOperation()
    .AddBatchHandler<WeatherForecast, WeatherForcastBatchHandlerService>(o =>
    {
        o.MaxItemPerBatch = 50;
        o.SlidingInterval = TimeSpan.FromSeconds(10);
    })
    .AddBatchHandler<AnotherWeatherForecast, AnotherWeatherForcastBatchHandlerService>(o =>
    {
        o.MaxItemPerBatch = 10;
    });

WebApplication webApplication = webApplicationBuilder.Build();

// Configure the HTTP request pipeline.
if (webApplication.Environment.IsDevelopment())
{
    webApplication.MapOpenApi();
}

webApplication.UseHttpsRedirection();

webApplication.UseAuthorization();

webApplication.MapControllers();

webApplication.Run();
