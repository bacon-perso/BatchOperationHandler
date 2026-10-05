namespace Bacon.BatchOperationHandler.Interfaces.Services;

/// <summary>
/// Batch operation client service interface. Allows to enqueue items into the queue to be processed after either a limit of items has been reached or a timer a deadlined
/// </summary>
/// <typeparam name="TItem">Batch item generic type</typeparam>
public interface IBatchOperationProcessorService<TItem>
{
    /// <summary>
    /// Enqueue batch item. Waits for free space when the queue is bounded and full.
    /// </summary>
    /// <param name="item">batch item</param>
    /// <param name="cancellationToken">Cancels the wait for free space</param>
    /// <returns>ValueTask</returns>
    /// <exception cref="System.Threading.Channels.ChannelClosedException">The service is shutting down.</exception>
    ValueTask EnqueueAsync(TItem item, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enqueue batch items. Waits for free space when the queue is bounded and full.
    /// </summary>
    /// <param name="items">List of items to batch</param>
    /// <param name="cancellationToken">Cancels the wait for free space</param>
    /// <returns>ValueTask</returns>
    /// <exception cref="System.Threading.Channels.ChannelClosedException">The service is shutting down.</exception>
    ValueTask EnqueueAsync(IEnumerable<TItem> items, CancellationToken cancellationToken = default);
}