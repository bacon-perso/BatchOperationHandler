namespace Bacon.BatchOperationHandler.Models;

/// <summary>
/// Batch operation handler options
/// </summary>
public sealed class BatchOperationHandlerOptions<TItem>
{
    /// <summary>
    /// Maximum number of batch item to process (default: 250 items)
    /// </summary>
    public ushort MaxItemPerBatch { get; set; } = 250;

    /// <summary>
    /// Sliding interval (default: 2 seconds). If items are endqueued and the MaxItemPerBatch is not reached, the queue will be processed once after the interval as been reached. Enqueueing an item resets the internal.
    /// </summary>
    public TimeSpan SlidingInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Maximum number of items waiting to be processed (default: unbounded). When set, enqueueing waits for free space once the limit is reached.
    /// </summary>
    public int? MaxQueueSize { get; set; }
}