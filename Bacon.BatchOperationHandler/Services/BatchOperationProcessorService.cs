using Bacon.BatchOperationHandler.Interfaces.Services;
using Bacon.BatchOperationHandler.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Channels;

namespace Bacon.BatchOperationHandler.Services;

internal sealed partial class BatchOperationProcessorService<TItem> : IHostedService, IBatchOperationProcessorService<TItem>, IDisposable
{
    #region DI

    #region Retry

    private static readonly TimeSpan _baseBackoff = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan _maxBackoff = TimeSpan.FromMinutes(1);

    #endregion Retry

    #region HostedService

    private readonly CancellationTokenSource _graceCancelationTokenSource = new();
    private readonly CancellationTokenSource _stoppingCancelationTokenSource = new(); // only used to skip backoff waits on shutdown
    private Task _loopTask = Task.CompletedTask;

    #endregion HostedService

    private readonly IBatchOperationHandler<TItem> _batchOperationHandler;

    // Used by the [LoggerMessage] source-generated methods; do not remove even if the IDE shows it as unused.
    private readonly ILogger<BatchOperationProcessorService<TItem>> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly Channel<TItem> _channel;
    private readonly int _maxItemPerBatch;
    private readonly TimeSpan _slidingInterval;


    public BatchOperationProcessorService(IBatchOperationHandler<TItem> batchOperationHandler, IOptions<BatchOperationHandlerOptions<TItem>> options, ILogger<BatchOperationProcessorService<TItem>> logger, TimeProvider timeProvider)
    {
        BatchOperationHandlerOptions<TItem> batchOperationHandlerOptions = options.Value;

        _batchOperationHandler = batchOperationHandler;
        _logger = logger;
        _timeProvider = timeProvider;

        _maxItemPerBatch = batchOperationHandlerOptions.MaxItemPerBatch;
        _slidingInterval = batchOperationHandlerOptions.SlidingInterval;

        _channel = batchOperationHandlerOptions.MaxQueueSize is int maxQueueSize 
            ? Channel.CreateBounded<TItem>(new BoundedChannelOptions(maxQueueSize) { SingleReader = true, FullMode = BoundedChannelFullMode.Wait }) 
            : Channel.CreateUnbounded<TItem>(new UnboundedChannelOptions { SingleReader = true });
    }

    #endregion DI

    #region Publics

    #region Enqueue

    public ValueTask EnqueueAsync(TItem item, CancellationToken cancellationToken = default)
    {
        return _channel.Writer.WriteAsync(item, cancellationToken);
    }

    public async ValueTask EnqueueAsync(IEnumerable<TItem> items, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(items);

        foreach (TItem item in items)
        {
            await _channel.Writer.WriteAsync(item, cancellationToken);
        }
    }

    #endregion Enqueue

    #region HostedService

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _loopTask = RunAsync();
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // No more items accepted; the loop drains what is queued and ends.
        _channel.Writer.TryComplete();

        // Wake the loop out of any backoff delay so it can drain.
        await _stoppingCancelationTokenSource.CancelAsync();

        // The host shutdown timeout is the grace period handlers are given to finish. Configured not to throw the expected OperationCancelledException
        await _loopTask.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        // Grace period over: cancel handlers explicitly (no Register callback, so no ordering race).
        if (cancellationToken.IsCancellationRequested)
        {
            await _graceCancelationTokenSource.CancelAsync();
        }
    }

    private async Task RunAsync()
    {
        // Return to the caller immediately; the loop continues on the thread pool. Without this, a backlog enqueued before start would be processed inline inside StartAsync.
        await Task.Yield();

        ChannelReader<TItem> reader = _channel.Reader;
        CancellationToken stoppingToken = _stoppingCancelationTokenSource.Token;
        int failures = 0;

        try
        {
            // Idle: no timer runs while the queue is empty. Ends when the writer is completed and drained.
            while (await reader.WaitToReadAsync(_graceCancelationTokenSource.Token))
            {
                List<TItem> batch = await ReadBatchAsync(reader);

                if (await ProcessBatchAsync(batch))
                {
                    failures = 0;
                }
                else
                {
                    await BackoffAsync(++failures, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (_graceCancelationTokenSource.IsCancellationRequested)
        {
            LogShutdownGracePeriodExpired();
        }
    }

    #endregion HostedService

    #region Dispose

    public void Dispose()
    {
        _graceCancelationTokenSource.Dispose();
        _stoppingCancelationTokenSource.Dispose();
    }

    #endregion Dispose

    #endregion Publics

    #region Privates

    #region Read Batch

    private async Task<List<TItem>> ReadBatchAsync(ChannelReader<TItem> reader)
    {
        List<TItem> batch = new(_maxItemPerBatch);

        using CancellationTokenSource deadline = new(_slidingInterval, _timeProvider);
        using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token, _graceCancelationTokenSource.Token);

        ///Fill a batch until it's either reached the max item per batch, or reached the timer deadline
        try
        {
            while (true)
            {
                bool added = false;

                while (batch.Count < _maxItemPerBatch && reader.TryRead(out TItem? item))
                {
                    batch.Add(item);
                    added = true;
                }

                if (added)
                {
                    deadline.CancelAfter(_slidingInterval);
                }

                if (batch.Count >= _maxItemPerBatch || !await reader.WaitToReadAsync(linked.Token))
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested && !_graceCancelationTokenSource.IsCancellationRequested)
        {
            // Deadline reached: flush what we have.
        }

        return batch;
    }

    #endregion Read Batch

    #region Process

    private async Task<bool> ProcessBatchAsync(List<TItem> batch)
    {
        try
        {
            await _batchOperationHandler.ProcessItemsAsync(batch, _graceCancelationTokenSource.Token);

            return true;
        }
        catch (Exception processException)
        {
            LogProcessItemsFailure(batch.Count, processException);

            try
            {
                await _batchOperationHandler.ProcessErrorsAsync(batch, processException, _graceCancelationTokenSource.Token);
            }
            catch (Exception errorException)
            {
                LogProcessErrorsFailure(errorException);
            }

            return false;
        }
    }

    #endregion Process

    #region Retry

    private async Task BackoffAsync(int failures, CancellationToken stoppingToken)
    {
        // equivalent to base × 2^(failures−1), capped at _maxBackoff. 1L is equivalent to (long)1
        long multiplier = 1L << Math.Min(failures - 1, 10);
        TimeSpan delay = TimeSpan.FromTicks(Math.Min(_baseBackoff.Ticks * multiplier, _maxBackoff.Ticks));

        await Task.Delay(delay, _timeProvider, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    #endregion Retry

    #region Logs

    [LoggerMessage(Level = LogLevel.Warning, Message = "Batch Operation Handler - Shutdown grace period expired; remaining batch items were not processed.")]
    private partial void LogShutdownGracePeriodExpired();

    [LoggerMessage(Level = LogLevel.Error, Message = "Batch Operation Handler - Failed to process a batch of {ItemCount} items.")]
    private partial void LogProcessItemsFailure(int itemCount, Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "Batch Operation Handler - Failed to process the failed batch items.")]
    private partial void LogProcessErrorsFailure(Exception exception);

    #endregion Logs

    #endregion Privates
}