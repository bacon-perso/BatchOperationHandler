# Bacon.BatchOperationHandler

A small library for .NET 10 that turns a stream of individual items into **batches**. Producers enqueue items one at a time (or in bulk). A background hosted service groups them and hands each group to your handler. A batch is flushed as soon as it is full, or when no new item has arrived for a configurable interval.

Typical uses: batching database inserts, bulk API calls, writing logs or metrics, or sending messages in groups instead of one by one.

## Features

- One independent queue, background loop and handler **per item type**.
- A batch is flushed when it reaches `MaxItemPerBatch`, or after the **sliding interval** with no new items.
- Optional **bounded queue** with backpressure: producers wait when the queue is full.
- Failed batches are passed to your `ProcessErrorsAsync`, and the loop backs off exponentially (1s up to 1 minute) before the next batch.
- **Graceful shutdown:** queued items are drained when the host stops, within the host's shutdown timeout.
- Options are validated at startup.
- Built on `TimeProvider`, so timing can be tested with a fake clock.

## Quick start

### 1. Implement a handler

A handler is a class implementing `IBatchOperationHandler<TItem>`. It is registered as a **singleton**, so it may take dependencies, but scoped services must be resolved through an `IServiceScopeFactory`.

```csharp
public class WeatherForcastBatchHandlerService : IBatchOperationHandler<WeatherForecast>
{
    public Task ProcessItemsAsync(IReadOnlyList<WeatherForecast> items, CancellationToken cancellationToken)
    {
        // Write the whole batch at once: one DB round trip, one HTTP call, ...
        Console.WriteLine($"Processed {items.Count} items");

        return Task.CompletedTask;
    }

    public Task ProcessErrorsAsync(IReadOnlyList<WeatherForecast> items, Exception exception, CancellationToken cancellationToken)
    {
        // Called with the same items that ProcessItemsAsync failed on.
        // You could requeue them, write them to a dead-letter store, log them, ...
        Console.WriteLine($"Batch of {items.Count} items failed: {exception.Message}");

        return Task.CompletedTask;
    }
}
```

### 2. Register it

```csharp
builder.Services.AddBatchOperation()
    .AddBatchHandler<WeatherForecast, WeatherForcastBatchHandlerService>(o =>
    {
        o.MaxItemPerBatch = 50;
        o.SlidingInterval = TimeSpan.FromSeconds(10);
    })
    .AddBatchHandler<AnotherWeatherForecast, AnotherWeatherForcastBatchHandlerService>(o =>
    {
        o.MaxItemPerBatch = 10;
    });
```

Each `AddBatchHandler<TItem, THandler>` call registers the handler, a queue and a hosted service for `TItem`. The options callback is optional, and defaults are used when it is omitted. Registering two handlers for the same `TItem` throws an `InvalidOperationException`.

### 3. Enqueue items

Inject `IBatchOperationProcessorService<TItem>` wherever you produce items:

```csharp
[ApiController]
[Route("batch")]
public class BatchController(IBatchOperationProcessorService<WeatherForecast> batchService) : ControllerBase
{
    [HttpPost("weather")]
    public async Task<IActionResult> Create(CancellationToken cancellationToken)
    {
        List<WeatherForecast> forecasts = [.. Enumerable.Range(0, 175).Select(_ => NewForecast())];

        // 175 items with MaxItemPerBatch = 50 -> batches of 50, 50, 50, then 25 after the interval.
        await batchService.EnqueueAsync(forecasts, cancellationToken);

        return Accepted();
    }
}
```

`EnqueueAsync` has two overloads: one for a single item and one for an `IEnumerable<TItem>`. Both return a `ValueTask`.

A complete runnable example lives in [`Bacon.BatchOperationHandler.Samples`](Bacon.BatchOperationHandler.Samples):

| File | Shows |
|---|---|
| `ProjectConfigs/Program.cs` | Registering two handlers with different options |
| `Services/WeatherForcastBatchHandlerService.cs` | A handler that randomly fails, to demonstrate `ProcessErrorsAsync` |
| `Services/AnotherWeatherForcastBatchHandlerService.cs` | A minimal handler |
| `Controllers/BatchController.cs` | Enqueueing a bulk list and a slow trickle of single items |

## Options

`BatchOperationHandlerOptions<TItem>`:

| Option | Default | Valid range | Description |
|---|---|---|---|
| `MaxItemPerBatch` | `250` | 1 – 1000 | Maximum items per batch. A full batch is flushed immediately. |
| `SlidingInterval` | `2 seconds` | 500 ms – 5 minutes | A partial batch is flushed once no item has been enqueued for this long. Each enqueued item resets the timer. |
| `MaxQueueSize` | `null` (unbounded) | `MaxItemPerBatch` – 100 000 | Maximum number of items waiting to be processed. When set, enqueueing waits for free space once the limit is reached. |

Options are validated when the host starts (`ValidateOnStart`), so an invalid configuration fails fast with an `OptionsValidationException` listing every problem.

## How it works

1. `EnqueueAsync` writes into an in-memory channel owned by that item type.
2. A background loop reads from the channel. While the queue is **empty, no timer runs**.
3. When the first item arrives a batch starts. The loop keeps adding items until either:
   - the batch holds `MaxItemPerBatch` items, which is flushed immediately, or
   - `SlidingInterval` elapses with no new item, which flushes the partial batch.
4. The batch is passed to `ProcessItemsAsync`. Batches are processed **one at a time, in order**. A handler is never called concurrently for the same item type.
5. If `ProcessItemsAsync` throws, the exception is logged and the same items go to `ProcessErrorsAsync`. The loop then waits before the next batch: 1s, 2s, 4s, ... capped at 1 minute. A successful batch resets the delay. Items are not retried automatically. Do that in `ProcessErrorsAsync` if you want to, for example by enqueueing them again.
6. An exception thrown by `ProcessErrorsAsync` is logged and swallowed.

### Backpressure

With the default unbounded queue, `EnqueueAsync` always completes immediately, but memory grows if the handler can't keep up. Set `MaxQueueSize` to bound it. Producers then wait (asynchronously) while the queue is full, and `EnqueueAsync` accepts a `CancellationToken` to abort that wait.

```csharp
.AddBatchHandler<Order, OrderBatchHandler>(o =>
{
    o.MaxItemPerBatch = 100;
    o.MaxQueueSize = 10_000;
})
```

### Shutdown

When the host stops, the queue stops accepting items: later `EnqueueAsync` calls throw `ChannelClosedException`. Everything already queued is drained, in batches and without waiting for the interval, and any pending failure backoff is skipped. The host's shutdown timeout is the grace period. When it expires, the `CancellationToken` passed to your handler is cancelled and the remaining items are abandoned, with a warning logged. Make handlers honor their token so shutdown stays fast.

> **Durability:** the queue lives in memory. Items still queued when the process crashes, or abandoned after the grace period, are lost.

## Things to know

- **Handlers are singletons** and are called from a background thread, never from the request that enqueued the item.
- **Don't pass a request-scoped `CancellationToken`** to `EnqueueAsync` if you enqueue from fire-and-forget work. The token is cancelled when the request ends, which would abort the enqueue.
- **One handler per item type.** To batch the same type for two purposes, wrap it in distinct types.
- `TimeProvider` is registered with `TryAddSingleton`. Register your own (for example `FakeTimeProvider`) before calling `AddBatchOperation()` to control time in tests.

## Testing

The test project `Bacon.BatchOperationHandler.Tests` uses NUnit and `Microsoft.Extensions.TimeProvider.Testing`. A `FakeTimeProvider` lets tests advance the sliding interval and the failure backoff without waiting.

```
dotnet test Bacon.BatchOperationHandler.Tests
```

## Requirements

- .NET 10
- `Microsoft.Extensions.Hosting` (the library registers an `IHostedService`)
