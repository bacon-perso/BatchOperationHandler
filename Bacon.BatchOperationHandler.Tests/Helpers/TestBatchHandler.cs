using Bacon.BatchOperationHandler.Interfaces.Services;
using System.Collections.Concurrent;

namespace Bacon.BatchOperationHandler.Tests.Helpers;

/// <summary>
/// Scriptable handler recording every call. Delegates can be overridden per test.
/// </summary>
internal sealed class TestBatchHandler<TItem> : IBatchOperationHandler<TItem>
{
    private readonly ConcurrentQueue<TItem[]> _batches = new();
    private readonly ConcurrentQueue<ErrorCall> _errorCalls = new();
    private readonly ConcurrentQueue<CancellationToken> _tokens = new();
    private int _processCallCount;

    public sealed record ErrorCall(TItem[] Items, Exception Exception, CancellationToken Token);

    /// <summary>Invoked on each ProcessItemsAsync, with the 1-based call number. Default: succeeds.</summary>
    public Func<int, IReadOnlyList<TItem>, CancellationToken, Task> OnProcessItems { get; set; } = (_, _, _) => Task.CompletedTask;

    /// <summary>Invoked on each ProcessErrorsAsync. Default: succeeds.</summary>
    public Func<IReadOnlyList<TItem>, Exception, CancellationToken, Task> OnProcessErrors { get; set; } = (_, _, _) => Task.CompletedTask;

    /// <summary>Invoked synchronously after a call has been recorded, before OnProcessItems.</summary>
    public Action<int>? OnCalled { get; set; }

    public IReadOnlyList<TItem[]> Batches => [.. _batches];

    public IReadOnlyList<ErrorCall> ErrorCalls => [.. _errorCalls];

    public IReadOnlyList<CancellationToken> Tokens => [.. _tokens];

    public int ProcessCallCount => Volatile.Read(ref _processCallCount);

    public IReadOnlyList<TItem> AllItems => [.. _batches.SelectMany(b => b)];

    public Task ProcessItemsAsync(IReadOnlyList<TItem> items, CancellationToken cancellationToken)
    {
        // Copy: the caller owns the list instance.
        _batches.Enqueue([.. items]);
        _tokens.Enqueue(cancellationToken);
        int call = Interlocked.Increment(ref _processCallCount);
        OnCalled?.Invoke(call);

        return OnProcessItems(call, items, cancellationToken);
    }

    public Task ProcessErrorsAsync(IReadOnlyList<TItem> items, Exception exception, CancellationToken cancellationToken)
    {
        _errorCalls.Enqueue(new ErrorCall([.. items], exception, cancellationToken));

        return OnProcessErrors(items, exception, cancellationToken);
    }
}
