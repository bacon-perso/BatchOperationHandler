namespace Bacon.BatchOperationHandler.Interfaces.Services;

/// <summary>
/// Batch operation handler interface. Implements what happens when the limit or timer is reached, or what to do in case of batch failure
/// </summary>
/// <typeparam name="TItem">Batch item generic type</typeparam>
public interface IBatchOperationHandler<TItem>
{
    /// <summary>
    /// Process batch items
    /// </summary>
    /// <param name="items">Batched items</param>
    /// <param name="cancellationToken">Signals that the host is stopping</param>
    /// <returns>Task</returns>
    Task ProcessItemsAsync(IReadOnlyList<TItem> items, CancellationToken cancellationToken);

    /// <summary>
    /// Handle error when batch process failed. Called with the same items that
    /// <see cref="ProcessItemsAsync"/> failed on; an exception thrown here is logged and swallowed.
    /// </summary>
    /// <param name="items">Batched items</param>
    /// <param name="exception">Thrown exception</param>
    /// <param name="cancellationToken">Signals that the host is stopping</param>
    /// <returns>Task</returns>
    Task ProcessErrorsAsync(IReadOnlyList<TItem> items, Exception exception, CancellationToken cancellationToken);
}