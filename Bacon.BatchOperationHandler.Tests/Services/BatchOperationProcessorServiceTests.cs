using Bacon.BatchOperationHandler.Interfaces.Services;
using Bacon.BatchOperationHandler.Models;
using Bacon.BatchOperationHandler.Services;
using Bacon.BatchOperationHandler.Tests.Helpers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace Bacon.BatchOperationHandler.Tests.Services;

[TestFixture]
public class BatchOperationProcessorServiceTests
{
    private static readonly TimeSpan _interval = TimeSpan.FromSeconds(2);

    private FakeTimeProvider _time = null!;
    private TestBatchHandler<string> _handler = null!;
    private TestLogger<BatchOperationProcessorService<string>> _logger = null!;
    private BatchOperationProcessorService<string>? _service;

    [SetUp]
    public void SetUp()
    {
        _time = new FakeTimeProvider();
        _handler = new TestBatchHandler<string>();
        _logger = new TestLogger<BatchOperationProcessorService<string>>();
        _service = null;
    }

    [TearDown]
    public async Task TearDown()
    {
        if (_service is null)
        {
            return;
        }

        using CancellationTokenSource cts = new(TimeSpan.FromSeconds(5));
        try
        {
            await _service.StopAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
        }

        _service.Dispose();
    }

    private BatchOperationProcessorService<string> Create(ushort maxItems = 3, TimeSpan? interval = null, int? maxQueueSize = null)
    {
        _service = new BatchOperationProcessorService<string>(
            _handler,
            Options.Create(new BatchOperationHandlerOptions<string> { MaxItemPerBatch = maxItems, SlidingInterval = interval ?? _interval, MaxQueueSize = maxQueueSize }),
            _logger,
            _time);

        return _service;
    }

    private async Task<BatchOperationProcessorService<string>> CreateAndStartAsync(ushort maxItems = 3, TimeSpan? interval = null, int? maxQueueSize = null)
    {
        BatchOperationProcessorService<string> service = Create(maxItems, interval, maxQueueSize);
        await service.StartAsync(CancellationToken.None);

        return service;
    }

    private bool GracePeriodWarningLogged() =>
        _logger.Entries.Any(e => e.Level == LogLevel.Warning && e.Message == "Batch Operation Handler - Shutdown grace period expired; remaining batch items were not processed.");

    private static Task WaitForCallsAsync(TestBatchHandler<string> handler, int count) =>
        AsyncTestHelper.WaitUntilAsync(() => handler.ProcessCallCount >= count, $"{count} handler call(s)");

    #region Enqueue (service not started)

    [Test]
    public void Enqueue_Unbounded_NeverWaits()
    {
        BatchOperationProcessorService<string> service = Create(maxQueueSize: null);

        for (int i = 0; i < 5_000; i++)
        {
            ValueTask task = service.EnqueueAsync($"item-{i}");
            Assert.That(task.IsCompletedSuccessfully, Is.True, $"enqueue #{i} should complete synchronously");
        }
    }

    [Test]
    public async Task Enqueue_Bounded_WaitsWhenFull_AndCompletesOnceConsumerFreesSpace()
    {
        BatchOperationProcessorService<string> service = Create(maxItems: 1, maxQueueSize: 1);

        await service.EnqueueAsync("a");
        Task pending = service.EnqueueAsync("b").AsTask();

        await AsyncTestHelper.SettleAsync();
        Assert.That(pending.IsCompleted, Is.False, "queue is full, second enqueue must wait");

        await service.StartAsync(CancellationToken.None);

        await pending.WithTimeout();
        await WaitForCallsAsync(_handler, 2);
        Assert.That(_handler.AllItems, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task Enqueue_BoundedAndFull_CancellationAbortsTheWait()
    {
        BatchOperationProcessorService<string> service = Create(maxItems: 1, maxQueueSize: 1);
        await service.EnqueueAsync("a");

        using CancellationTokenSource cts = new();
        Task pending = service.EnqueueAsync("b", cts.Token).AsTask();
        await AsyncTestHelper.SettleAsync();
        Assert.That(pending.IsCompleted, Is.False);

        await cts.CancelAsync();

        Assert.CatchAsync<OperationCanceledException>(async () => await pending.WithTimeout());
    }

    [Test]
    public void Enqueue_BoundedAndFull_AlreadyCancelledToken_Throws()
    {
        BatchOperationProcessorService<string> service = Create(maxItems: 1, maxQueueSize: 1);

        Assert.CatchAsync<OperationCanceledException>(async () => await service.EnqueueAsync("a", new CancellationToken(true)));
    }

    [Test]
    public void Enqueue_Single_AfterCancellationDoesNotEnqueueTheItem()
    {
        BatchOperationProcessorService<string> service = Create(maxItems: 1, maxQueueSize: 1);

        Assert.CatchAsync<OperationCanceledException>(async () => await service.EnqueueAsync("lost", new CancellationToken(true)));

        // Queue is still empty: a normal enqueue completes synchronously.
        Assert.That(service.EnqueueAsync("kept").IsCompletedSuccessfully, Is.True);
    }

    [Test]
    public void Enqueue_NullCollection_ThrowsArgumentNullException()
    {
        BatchOperationProcessorService<string> service = Create();

        Assert.ThrowsAsync<ArgumentNullException>(async () => await service.EnqueueAsync((IEnumerable<string>)null!));
    }

    [Test]
    public async Task Enqueue_EmptyCollection_DoesNothing()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync();

        await service.EnqueueAsync(Array.Empty<string>());
        await AsyncTestHelper.SettleAsync();
        _time.Advance(TimeSpan.FromMinutes(10));
        await AsyncTestHelper.SettleAsync();

        Assert.That(_handler.ProcessCallCount, Is.Zero);
    }

    [Test]
    public async Task Enqueue_Collection_PreservesOrder()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 100);

        await service.EnqueueAsync(Enumerable.Range(0, 50).Select(i => i.ToString()));
        await service.StopAsync(CancellationToken.None);

        Assert.That(_handler.AllItems, Is.EqualTo(Enumerable.Range(0, 50).Select(i => i.ToString())));
    }

    [Test]
    public async Task Enqueue_Collection_BoundedQueueSmallerThanCollection_WaitsAndDeliversAll()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 2, maxQueueSize: 2);

        string[] items = [.. Enumerable.Range(0, 20).Select(i => i.ToString())];
        await service.EnqueueAsync(items).AsTask().WithTimeout();
        await service.StopAsync(CancellationToken.None);

        Assert.That(_handler.AllItems, Is.EqualTo(items));
    }

    [Test]
    public async Task Enqueue_Collection_EnumeratesLazilyOnce()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 100);
        int enumerations = 0;

        IEnumerable<string> Source()
        {
            enumerations++;
            yield return "x";
            yield return "y";
        }

        await service.EnqueueAsync(Source());
        await service.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(enumerations, Is.EqualTo(1));
            Assert.That(_handler.AllItems, Is.EqualTo(new[] { "x", "y" }));
        });
    }

    [Test]
    public async Task Enqueue_Collection_SourceThrowing_KeepsItemsAlreadyEnqueued()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 100);

        IEnumerable<string> Source()
        {
            yield return "x";
            throw new FormatException("boom");
        }

        Assert.ThrowsAsync<FormatException>(async () => await service.EnqueueAsync(Source()));
        await service.StopAsync(CancellationToken.None);

        Assert.That(_handler.AllItems, Is.EqualTo(new[] { "x" }));
    }

    [Test]
    public async Task Enqueue_AfterStop_ThrowsChannelClosedException()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync();
        await service.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ChannelClosedException>(async () => await service.EnqueueAsync("late"));
            Assert.ThrowsAsync<ChannelClosedException>(async () => await service.EnqueueAsync(["late"]));
        });
    }

    [Test]
    public async Task Enqueue_AfterStopWithEmptyCollection_DoesNotThrow()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync();
        await service.StopAsync(CancellationToken.None);

        await service.EnqueueAsync(Array.Empty<string>());
    }

    [Test]
    public async Task Enqueue_BoundedBackpressure_BlocksProducerWhileHandlerIsBusy_ThenResumes()
    {
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _handler.OnProcessItems = (_, _, _) => release.Task;
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1, maxQueueSize: 2);

        // First item goes in-flight to the (blocked) handler; the loop then reads nothing more.
        await service.EnqueueAsync("i0");
        await WaitForCallsAsync(_handler, 1);

        // The queue holds exactly 2 more items; the next enqueue must wait.
        Assert.That(service.EnqueueAsync("i1").IsCompletedSuccessfully, Is.True);
        Assert.That(service.EnqueueAsync("i2").IsCompletedSuccessfully, Is.True);
        Task throttled = service.EnqueueAsync("i3").AsTask();

        await AsyncTestHelper.SettleAsync();
        Assert.That(throttled.IsCompleted, Is.False, "producer should be throttled while the handler is busy");

        release.SetResult();

        await throttled.WithTimeout();
        await service.StopAsync(CancellationToken.None);
        Assert.That(_handler.AllItems, Is.EqualTo(new[] { "i0", "i1", "i2", "i3" }));
    }

    #endregion Enqueue (service not started)

    #region Batching - size

    [Test]
    public async Task Batch_ReachingMaxItems_FlushesImmediately_WithoutTimePassing()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 3);

        await service.EnqueueAsync(["a", "b", "c"]);

        await WaitForCallsAsync(_handler, 1);
        Assert.That(_handler.Batches[0], Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public async Task Batch_MaxItemsOfOne_FlushesEveryItemSeparately()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);

        await service.EnqueueAsync(["a", "b", "c"]);

        await WaitForCallsAsync(_handler, 3);
        Assert.That(_handler.Batches, Is.EqualTo(new[] { new[] { "a" }, new[] { "b" }, new[] { "c" } }));
    }

    [Test]
    public async Task Batch_MoreItemsThanMax_SplitsIntoFullBatches_AndKeepsRemainderForTimer()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 3);

        await service.EnqueueAsync(["1", "2", "3", "4", "5", "6", "7"]);

        await WaitForCallsAsync(_handler, 2);
        await AsyncTestHelper.SettleAsync();
        Assert.That(_handler.Batches, Is.EqualTo(new[] { new[] { "1", "2", "3" }, new[] { "4", "5", "6" } }));

        _time.Advance(_interval);

        await WaitForCallsAsync(_handler, 3);
        Assert.That(_handler.Batches[2], Is.EqualTo(new[] { "7" }));
    }

    [Test]
    public async Task Batch_NeverExceedsMaxItemsPerBatch()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 7);

        await service.EnqueueAsync(Enumerable.Range(0, 100).Select(i => i.ToString()));
        await service.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(_handler.Batches.Max(b => b.Length), Is.LessThanOrEqualTo(7));
            Assert.That(_handler.AllItems, Has.Count.EqualTo(100));
            Assert.That(_handler.Batches.Count, Is.EqualTo(15)); // 14 full + 1 remainder
        });
    }

    [Test]
    public async Task Batch_LargeMaxItems_Works()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1000);

        await service.EnqueueAsync(Enumerable.Range(0, 1000).Select(i => i.ToString()));

        await WaitForCallsAsync(_handler, 1);
        Assert.That(_handler.Batches[0], Has.Length.EqualTo(1000));
    }

    [Test]
    public async Task Batch_SingleItemAfterSuccessfulBatch_StartsNewBatchWithFreshTimer()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 3);
        await service.EnqueueAsync("a");
        await AsyncTestHelper.SettleAsync();
        _time.Advance(_interval);
        await WaitForCallsAsync(_handler, 1);

        await service.EnqueueAsync("b");
        await AsyncTestHelper.SettleAsync();
        _time.Advance(_interval - TimeSpan.FromMilliseconds(100));
        await AsyncTestHelper.SettleAsync();
        Assert.That(_handler.ProcessCallCount, Is.EqualTo(1), "second batch must wait its own full interval");

        _time.Advance(TimeSpan.FromMilliseconds(100));

        await WaitForCallsAsync(_handler, 2);
        Assert.That(_handler.Batches, Is.EqualTo(new[] { new[] { "a" }, new[] { "b" } }));
    }

    #endregion Batching - size

    #region Batching - timer

    [Test]
    public async Task Timer_PartialBatch_NotFlushedBeforeInterval()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 3);

        await service.EnqueueAsync(["a", "b"]);
        await AsyncTestHelper.SettleAsync();
        _time.Advance(_interval - TimeSpan.FromMilliseconds(100));
        await AsyncTestHelper.SettleAsync();

        Assert.That(_handler.ProcessCallCount, Is.Zero);
    }

    [Test]
    public async Task Timer_PartialBatch_FlushedWhenIntervalElapses()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 3);

        await service.EnqueueAsync(["a", "b"]);
        await AsyncTestHelper.SettleAsync();
        _time.Advance(_interval);

        await WaitForCallsAsync(_handler, 1);
        Assert.That(_handler.Batches[0], Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task Timer_SingleItem_IsFlushedByTimer()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 250);

        await service.EnqueueAsync("only");
        await AsyncTestHelper.SettleAsync();
        _time.Advance(_interval);

        await WaitForCallsAsync(_handler, 1);
        Assert.That(_handler.Batches[0], Is.EqualTo(new[] { "only" }));
    }

    [Test]
    public async Task Timer_Sliding_EnqueueingResetsTheInterval()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 5);

        await service.EnqueueAsync("a");
        await AsyncTestHelper.SettleAsync();
        _time.Advance(TimeSpan.FromMilliseconds(1500));

        await service.EnqueueAsync("b"); // resets the 2s window
        await AsyncTestHelper.SettleAsync();
        _time.Advance(TimeSpan.FromMilliseconds(1500)); // 3s since first item, 1.5s since the last
        await AsyncTestHelper.SettleAsync();
        Assert.That(_handler.ProcessCallCount, Is.Zero, "interval is sliding, must not have flushed yet");

        _time.Advance(TimeSpan.FromMilliseconds(500));

        await WaitForCallsAsync(_handler, 1);
        Assert.That(_handler.Batches[0], Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task Timer_Sliding_ContinuousTrickleKeepsBatchOpenUntilMaxReached()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 4);

        for (int i = 0; i < 3; i++)
        {
            await service.EnqueueAsync($"i{i}");
            await AsyncTestHelper.SettleAsync();
            _time.Advance(TimeSpan.FromMilliseconds(1900));
            await AsyncTestHelper.SettleAsync();
        }

        Assert.That(_handler.ProcessCallCount, Is.Zero);

        await service.EnqueueAsync("i3"); // reaches max

        await WaitForCallsAsync(_handler, 1);
        Assert.That(_handler.Batches[0], Is.EqualTo(new[] { "i0", "i1", "i2", "i3" }));
    }

    [Test]
    public async Task Timer_CustomInterval_IsHonoured()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 10, interval: TimeSpan.FromSeconds(30));

        await service.EnqueueAsync("a");
        await AsyncTestHelper.SettleAsync();
        _time.Advance(TimeSpan.FromSeconds(29));
        await AsyncTestHelper.SettleAsync();
        Assert.That(_handler.ProcessCallCount, Is.Zero);

        _time.Advance(TimeSpan.FromSeconds(1));

        await WaitForCallsAsync(_handler, 1);
    }

    [Test]
    public async Task Timer_Idle_NoTimerRuns_AndNoHandlerCalls()
    {
        await CreateAndStartAsync();

        _time.Advance(TimeSpan.FromHours(1));
        await AsyncTestHelper.SettleAsync();

        Assert.That(_handler.ProcessCallCount, Is.Zero);
    }

    [Test]
    public async Task Timer_AfterLongIdle_StartsOnlyWhenFirstItemArrives()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 3);

        _time.Advance(TimeSpan.FromHours(1));
        await AsyncTestHelper.SettleAsync();

        await service.EnqueueAsync("late");
        await AsyncTestHelper.SettleAsync();
        _time.Advance(_interval - TimeSpan.FromMilliseconds(100));
        await AsyncTestHelper.SettleAsync();
        Assert.That(_handler.ProcessCallCount, Is.Zero);

        _time.Advance(TimeSpan.FromMilliseconds(100));

        await WaitForCallsAsync(_handler, 1);
        Assert.That(_handler.Batches[0], Is.EqualTo(new[] { "late" }));
    }

    [Test]
    public async Task Timer_FlushesRemainderAfterFullBatchesWereProcessed()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 2);

        await service.EnqueueAsync(["a", "b", "c"]);
        await WaitForCallsAsync(_handler, 1);
        await AsyncTestHelper.SettleAsync();
        _time.Advance(_interval);

        await WaitForCallsAsync(_handler, 2);
        Assert.That(_handler.Batches, Is.EqualTo(new[] { new[] { "a", "b" }, new[] { "c" } }));
    }

    #endregion Batching - timer

    #region Handler contract

    [Test]
    public async Task Handler_IsNeverInvokedConcurrently()
    {
        int concurrent = 0;
        int maxConcurrent = 0;
        _handler.OnProcessItems = async (_, _, _) =>
        {
            int now = Interlocked.Increment(ref concurrent);
            int snapshot;
            while ((snapshot = Volatile.Read(ref maxConcurrent)) < now && Interlocked.CompareExchange(ref maxConcurrent, now, snapshot) != snapshot)
            {
            }

            await Task.Delay(5);
            Interlocked.Decrement(ref concurrent);
        };
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 2);

        await service.EnqueueAsync(Enumerable.Range(0, 40).Select(i => i.ToString()));
        await service.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(maxConcurrent, Is.EqualTo(1));
            Assert.That(_handler.AllItems, Has.Count.EqualTo(40));
        });
    }

    [Test]
    public async Task Handler_ReceivesLiveToken_NotCancelledDuringNormalOperation()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);

        await service.EnqueueAsync("a");

        await WaitForCallsAsync(_handler, 1);
        Assert.Multiple(() =>
        {
            Assert.That(_handler.Tokens[0].CanBeCanceled, Is.True);
            Assert.That(_handler.Tokens[0].IsCancellationRequested, Is.False);
        });
    }

    [Test]
    public async Task Handler_SlowHandler_DoesNotLoseItemsEnqueuedMeanwhile()
    {
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _handler.OnProcessItems = (call, _, _) => call == 1 ? release.Task : Task.CompletedTask;
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 2);

        await service.EnqueueAsync(["a", "b"]);
        await WaitForCallsAsync(_handler, 1);
        await service.EnqueueAsync(["c", "d", "e", "f"]);
        release.SetResult();
        await service.StopAsync(CancellationToken.None);

        Assert.That(_handler.AllItems, Is.EqualTo(new[] { "a", "b", "c", "d", "e", "f" }));
    }

    [Test]
    public async Task Handler_GetsReadOnlyListSnapshotOfBatch()
    {
        IReadOnlyList<string>? received = null;
        _handler.OnProcessItems = (_, items, _) =>
        {
            received = items;

            return Task.CompletedTask;
        };
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 2);

        await service.EnqueueAsync(["a", "b"]);

        await WaitForCallsAsync(_handler, 1);
        Assert.Multiple(() =>
        {
            Assert.That(received, Is.Not.Null);
            Assert.That(received, Is.EqualTo(new[] { "a", "b" }));
        });
    }

    #endregion Handler contract

    #region Failures

    [Test]
    public async Task Failure_ProcessErrorsCalledWithSameItemsAndException()
    {
        InvalidOperationException failure = new("handler failed");
        _handler.OnProcessItems = (_, _, _) => throw failure;
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 2);

        await service.EnqueueAsync(["a", "b"]);

        await AsyncTestHelper.WaitUntilAsync(() => _handler.ErrorCalls.Count == 1, "error handler call");
        Assert.Multiple(() =>
        {
            Assert.That(_handler.ErrorCalls[0].Items, Is.EqualTo(new[] { "a", "b" }));
            Assert.That(_handler.ErrorCalls[0].Exception, Is.SameAs(failure));
            Assert.That(_handler.ErrorCalls[0].Token.IsCancellationRequested, Is.False);
        });
    }

    [Test]
    public async Task Failure_AsyncFaultedTask_IsAlsoHandled()
    {
        _handler.OnProcessItems = async (_, _, _) =>
        {
            await Task.Yield();
            throw new TimeoutException("async failure");
        };
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);

        await service.EnqueueAsync("a");

        await AsyncTestHelper.WaitUntilAsync(() => _handler.ErrorCalls.Count == 1, "error handler call");
        Assert.That(_handler.ErrorCalls[0].Exception, Is.TypeOf<TimeoutException>());
    }

    [Test]
    public async Task Failure_IsLoggedAsErrorWithExceptionAndItemCount()
    {
        InvalidOperationException failure = new("handler failed");
        _handler.OnProcessItems = (_, _, _) => throw failure;
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 2);

        await service.EnqueueAsync(["a", "b"]);

        await AsyncTestHelper.WaitUntilAsync(() => _logger.Entries.Any(e => e.Level == LogLevel.Error), "error log");
        TestLogger<BatchOperationProcessorService<string>>.Entry entry = _logger.Entries.Single(e => e.Level == LogLevel.Error);
        Assert.Multiple(() =>
        {
            Assert.That(entry.Exception, Is.SameAs(failure));
            Assert.That(entry.Message, Is.EqualTo("Batch Operation Handler - Failed to process a batch of 2 items."));
        });
    }

    [Test]
    public async Task Failure_ProcessErrorsThrowing_IsLoggedAndSwallowed_ServiceKeepsRunning()
    {
        InvalidOperationException errorHandlerFailure = new("error handler failed");
        _handler.OnProcessItems = (call, _, _) => call == 1 ? throw new InvalidOperationException("first fails") : Task.CompletedTask;
        _handler.OnProcessErrors = (_, _, _) => throw errorHandlerFailure;
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);

        await service.EnqueueAsync(["a", "b"]);
        await WaitForCallsAsync(_handler, 1);
        await AsyncTestHelper.AdvanceUntilAsync(_time, () => _handler.ProcessCallCount >= 2, "second batch after backoff", TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(_handler.Batches, Is.EqualTo(new[] { new[] { "a" }, new[] { "b" } }));
            Assert.That(_logger.Entries.Count(e => e.Level == LogLevel.Error), Is.EqualTo(2));
            Assert.That(_logger.Entries.Any(e => ReferenceEquals(e.Exception, errorHandlerFailure) && e.Message == "Batch Operation Handler - Failed to process the failed batch items."), Is.True);
        });
    }

    [Test]
    public async Task Failure_ProcessErrorsAsyncFault_IsAlsoSwallowed()
    {
        _handler.OnProcessItems = (_, _, _) => throw new InvalidOperationException("fails");
        _handler.OnProcessErrors = async (_, _, _) =>
        {
            await Task.Yield();
            throw new InvalidOperationException("error handler fails async");
        };
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);

        await service.EnqueueAsync("a");

        await AsyncTestHelper.WaitUntilAsync(() => _logger.Entries.Count(e => e.Level == LogLevel.Error) == 2, "both errors logged");

        // The loop must survive: an item enqueued afterwards is still processed (after the backoff).
        _handler.OnProcessItems = (_, _, _) => Task.CompletedTask;
        await service.EnqueueAsync("b");
        await AsyncTestHelper.AdvanceUntilAsync(_time, () => _handler.ProcessCallCount >= 2, "second batch processed", TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(10));

        Assert.That(_handler.Batches, Is.EqualTo(new[] { new[] { "a" }, new[] { "b" } }));
    }

    [Test]
    public async Task Failure_FailedBatchIsNotRetriedByTheService()
    {
        _handler.OnProcessItems = (_, _, _) => throw new InvalidOperationException("fails");
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);

        await service.EnqueueAsync("a");
        await WaitForCallsAsync(_handler, 1);
        _time.Advance(TimeSpan.FromMinutes(5));
        await AsyncTestHelper.SettleAsync();

        Assert.That(_handler.ProcessCallCount, Is.EqualTo(1), "failures are handed to ProcessErrorsAsync, not retried");
    }

    [Test]
    public async Task Failure_NextBatchWaitsForBackoff_OfOneSecond()
    {
        _handler.OnProcessItems = (call, _, _) => call == 1 ? throw new InvalidOperationException("fails") : Task.CompletedTask;
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);

        await service.EnqueueAsync(["a", "b"]);
        await WaitForCallsAsync(_handler, 1);
        await AsyncTestHelper.SettleAsync();

        _time.Advance(TimeSpan.FromMilliseconds(900));
        await AsyncTestHelper.SettleAsync();
        Assert.That(_handler.ProcessCallCount, Is.EqualTo(1), "still backing off");

        _time.Advance(TimeSpan.FromMilliseconds(100));

        await WaitForCallsAsync(_handler, 2);
        Assert.That(_handler.Batches[1], Is.EqualTo(new[] { "b" }));
    }

    [Test]
    public async Task Failure_BackoffGrowsExponentially_AndIsCappedAtOneMinute()
    {
        List<DateTimeOffset> callTimes = [];
        _handler.OnCalled = _ => { lock (callTimes) { callTimes.Add(_time.GetUtcNow()); } };
        _handler.OnProcessItems = (_, _, _) => throw new InvalidOperationException("always fails");
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);

        const int calls = 9;
        await service.EnqueueAsync(Enumerable.Range(0, calls).Select(i => i.ToString()));
        await AsyncTestHelper.AdvanceUntilAsync(_time, () => _handler.ProcessCallCount >= calls, "all failing batches", TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(10));

        double[] gaps;
        lock (callTimes)
        {
            gaps = [.. callTimes.Zip(callTimes.Skip(1), (a, b) => (b - a).TotalSeconds)];
        }

        // base 1s, doubling, capped at 60s (2^6 = 64 would exceed the cap).
        double[] expected = [1, 2, 4, 8, 16, 32, 60, 60];
        Assert.That(gaps, Has.Length.EqualTo(expected.Length));
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.That(gaps[i], Is.EqualTo(expected[i]).Within(1), $"backoff before call #{i + 2}");
        }
    }

    [Test]
    public async Task Failure_SuccessResetsBackoff()
    {
        List<DateTimeOffset> callTimes = [];
        _handler.OnCalled = _ => { lock (callTimes) { callTimes.Add(_time.GetUtcNow()); } };
        // fail, fail, succeed, fail, succeed
        _handler.OnProcessItems = (call, _, _) => call is 1 or 2 or 4 ? throw new InvalidOperationException("fails") : Task.CompletedTask;
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);

        await service.EnqueueAsync(["a", "b", "c", "d", "e"]);
        await AsyncTestHelper.AdvanceUntilAsync(_time, () => _handler.ProcessCallCount >= 5, "five calls", TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(2));

        double[] gaps;
        lock (callTimes)
        {
            gaps = [.. callTimes.Zip(callTimes.Skip(1), (x, y) => (y - x).TotalSeconds)];
        }

        Assert.Multiple(() =>
        {
            Assert.That(gaps[0], Is.EqualTo(1).Within(1), "backoff after 1st failure");
            Assert.That(gaps[1], Is.EqualTo(2).Within(1), "backoff after 2nd consecutive failure");
            Assert.That(gaps[3], Is.EqualTo(1).Within(1), "after a success the next failure starts again at 1s, not 4s");
        });
    }

    [Test]
    public async Task Failure_ItemsEnqueuedWhileBackingOff_AreProcessedAfterwards()
    {
        _handler.OnProcessItems = (call, _, _) => call == 1 ? throw new InvalidOperationException("fails") : Task.CompletedTask;
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);

        await service.EnqueueAsync("a");
        await WaitForCallsAsync(_handler, 1);
        await service.EnqueueAsync("b");
        await AsyncTestHelper.AdvanceUntilAsync(_time, () => _handler.ProcessCallCount >= 2, "processed after backoff", TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(10));

        Assert.That(_handler.AllItems, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task Failure_OnlyFailingBatchIsReportedToProcessErrors()
    {
        _handler.OnProcessItems = (call, _, _) => call == 2 ? throw new InvalidOperationException("second fails") : Task.CompletedTask;
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 2);

        await service.EnqueueAsync(["a", "b", "c", "d", "e", "f"]);
        await AsyncTestHelper.AdvanceUntilAsync(_time, () => _handler.ProcessCallCount >= 3, "three batches", TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(10));

        Assert.Multiple(() =>
        {
            Assert.That(_handler.ErrorCalls, Has.Count.EqualTo(1));
            Assert.That(_handler.ErrorCalls[0].Items, Is.EqualTo(new[] { "c", "d" }));
        });
    }

    #endregion Failures

    #region Shutdown

    [Test]
    public async Task Stop_WhenIdle_CompletesPromptly_WithoutHandlerCalls()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync();

        await service.StopAsync(CancellationToken.None).WithTimeout();

        Assert.Multiple(() =>
        {
            Assert.That(_handler.ProcessCallCount, Is.Zero);
            Assert.That(_logger.Entries, Is.Empty);
        });
    }

    [Test]
    public async Task Stop_FlushesPartialBatch_WithoutWaitingForTimer()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 10);

        await service.EnqueueAsync(["a", "b"]);
        await service.StopAsync(CancellationToken.None).WithTimeout();

        // Fake clock never advanced: the flush is driven by shutdown, not the timer.
        Assert.That(_handler.Batches, Is.EqualTo(new[] { new[] { "a", "b" } }));
    }

    [Test]
    public async Task Stop_DrainsEverythingQueued_InMaxSizedBatches()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 2);

        await service.EnqueueAsync(["a", "b", "c", "d", "e"]);
        await service.StopAsync(CancellationToken.None).WithTimeout();

        Assert.That(_handler.AllItems, Is.EqualTo(new[] { "a", "b", "c", "d", "e" }));
        Assert.That(_handler.Batches.All(b => b.Length <= 2), Is.True);
    }

    [Test]
    public async Task Stop_DuringBackoff_SkipsTheWait_AndStillDrainsQueue()
    {
        _handler.OnProcessItems = (call, _, _) => call == 1 ? throw new InvalidOperationException("fails") : Task.CompletedTask;
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);

        await service.EnqueueAsync("a");
        await WaitForCallsAsync(_handler, 1);
        await AsyncTestHelper.SettleAsync(); // loop is now in its 1s backoff
        await service.EnqueueAsync("b");

        await service.StopAsync(CancellationToken.None).WithTimeout();

        Assert.That(_handler.AllItems, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task Stop_CalledTwice_IsHarmless()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync();
        await service.EnqueueAsync("a");

        await service.StopAsync(CancellationToken.None);
        await service.StopAsync(CancellationToken.None);

        Assert.That(_handler.AllItems, Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public async Task Stop_WithoutStart_ClosesTheQueue()
    {
        BatchOperationProcessorService<string> service = Create();

        await service.StopAsync(CancellationToken.None);

        Assert.ThrowsAsync<ChannelClosedException>(async () => await service.EnqueueAsync("late"));
    }

    [Test]
    public async Task Stop_WaitsForInFlightHandler_ToFinish()
    {
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _handler.OnProcessItems = (_, _, _) => release.Task;
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);
        await service.EnqueueAsync("a");
        await WaitForCallsAsync(_handler, 1);

        Task stop = service.StopAsync(CancellationToken.None);
        await AsyncTestHelper.SettleAsync();
        Assert.That(stop.IsCompleted, Is.False, "host waits for the handler within the grace period");

        release.SetResult();

        await stop.WithTimeout();
        Assert.That(_handler.Tokens[0].IsCancellationRequested, Is.False, "handler finished within the grace period");
    }

    [Test]
    public async Task Stop_GracePeriodAlreadyExpired_CancelsHandlerToken_AbandonsQueue_AndLogsWarning()
    {
        _handler.OnProcessItems = async (_, _, ct) => await Task.Delay(Timeout.Infinite, ct);
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);
        await service.EnqueueAsync(["a", "b"]);
        await WaitForCallsAsync(_handler, 1);

        using CancellationTokenSource expired = new();
        await expired.CancelAsync();
        await service.StopAsync(expired.Token).WithTimeout();

        await AsyncTestHelper.WaitUntilAsync(GracePeriodWarningLogged, "grace period warning (loop ended)");
        Assert.Multiple(() =>
        {
            Assert.That(_handler.Tokens[0].IsCancellationRequested, Is.True, "handler token is cancelled when the grace period ends");
            Assert.That(_handler.ProcessCallCount, Is.EqualTo(1), "second batch is abandoned");
            Assert.That(_handler.ErrorCalls, Has.Count.EqualTo(1));
            Assert.That(_handler.ErrorCalls[0].Items, Is.EqualTo(new[] { "a" }));
            Assert.That(_handler.ErrorCalls[0].Exception, Is.InstanceOf<OperationCanceledException>());
            Assert.That(_logger.Entries.Count(e => e.Level == LogLevel.Error), Is.EqualTo(1), "only the cancelled handler call is logged as failure");
        });
    }

    [Test]
    public async Task Stop_GracePeriodExpiresLater_StopsWaitingWhenTokenIsCancelled()
    {
        _handler.OnProcessItems = async (_, _, ct) => await Task.Delay(Timeout.Infinite, ct);
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);
        await service.EnqueueAsync("a");
        await WaitForCallsAsync(_handler, 1);

        using CancellationTokenSource grace = new();
        Task stop = service.StopAsync(grace.Token);
        await AsyncTestHelper.SettleAsync();
        Assert.That(stop.IsCompleted, Is.False);
        Assert.That(_handler.Tokens[0].IsCancellationRequested, Is.False);

        await grace.CancelAsync();

        await stop.WithTimeout();
        await AsyncTestHelper.WaitUntilAsync(() => _handler.Tokens[0].IsCancellationRequested, "handler token cancelled");
        await AsyncTestHelper.WaitUntilAsync(GracePeriodWarningLogged, "grace period warning (loop ended)");
    }

    [Test]
    public async Task Stop_ImmediatelyAfterStart_StillDrainsEverythingQueued()
    {
        BatchOperationProcessorService<string> service = Create(maxItems: 2);
        await service.EnqueueAsync(["a", "b", "c"]);

        await service.StartAsync(CancellationToken.None);
        await service.StopAsync(CancellationToken.None).WithTimeout();

        Assert.That(_handler.Batches, Is.EqualTo(new[] { new[] { "a", "b" }, new[] { "c" } }));
    }

    [Test]
    public async Task Start_WithLargeBacklog_DoesNotProcessInlineInStartAsync()
    {
        int callingThreadCalls = 0;
        int startThreadId = Environment.CurrentManagedThreadId;
        _handler.OnProcessItems = (_, _, _) =>
        {
            if (Environment.CurrentManagedThreadId == startThreadId)
            {
                callingThreadCalls++;
            }

            return Task.CompletedTask;
        };
        BatchOperationProcessorService<string> service = Create(maxItems: 1);
        await service.EnqueueAsync(Enumerable.Range(0, 50).Select(i => i.ToString()));

        await service.StartAsync(CancellationToken.None);
        await service.StopAsync(CancellationToken.None).WithTimeout();

        Assert.Multiple(() =>
        {
            Assert.That(_handler.ProcessCallCount, Is.EqualTo(50));
            Assert.That(callingThreadCalls, Is.Zero, "the backlog must be processed off the caller's thread, not inline in StartAsync");
        });
    }

    [Test]
    public async Task Stop_GracePeriodExpiresWhileWaitingForItems_DoesNotThrow_AndLogsWarning()
    {
        // The loop is parked in the middle of building a batch when the grace token fires.
        _handler.OnProcessItems = async (call, _, ct) =>
        {
            if (call == 1)
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
        };
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 1);
        await service.EnqueueAsync(["a", "b", "c"]);
        await WaitForCallsAsync(_handler, 1);

        using CancellationTokenSource expired = new();
        await expired.CancelAsync();
        await service.StopAsync(expired.Token);

        await AsyncTestHelper.WaitUntilAsync(GracePeriodWarningLogged, "grace period warning (loop ended)");
        Assert.That(_logger.Entries.Count(e => e.Level == LogLevel.Warning), Is.EqualTo(1));
    }

    [Test]
    public async Task Stop_NormalShutdown_DoesNotLogWarning()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 10);
        await service.EnqueueAsync(["a", "b"]);

        await service.StopAsync(CancellationToken.None);

        Assert.That(_logger.Entries.Where(e => e.Level >= LogLevel.Warning), Is.Empty);
    }

    [Test]
    public async Task Stop_HandlerFailsDuringDrain_ProcessErrorsStillInvoked()
    {
        _handler.OnProcessItems = (_, _, _) => throw new InvalidOperationException("fails during drain");
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 10);
        await service.EnqueueAsync(["a", "b"]);

        await service.StopAsync(CancellationToken.None).WithTimeout();

        Assert.Multiple(() =>
        {
            Assert.That(_handler.ErrorCalls, Has.Count.EqualTo(1));
            Assert.That(_handler.ErrorCalls[0].Items, Is.EqualTo(new[] { "a", "b" }));
        });
    }

    #endregion Shutdown

    #region Lifecycle

    [Test]
    public async Task Dispose_AfterStop_DoesNotThrow_AndIsIdempotent()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync();
        await service.StopAsync(CancellationToken.None);

        _service = null; // disposed here; the TearDown must not stop it again

        Assert.DoesNotThrow(service.Dispose);
        Assert.DoesNotThrow(service.Dispose);
    }

    [Test]
    public void Dispose_NeverStarted_DoesNotThrow()
    {
        BatchOperationProcessorService<string> service = Create();
        _service = null; // disposed here; the TearDown must not stop it again

        Assert.DoesNotThrow(service.Dispose);
    }

    [Test]
    public void Service_ImplementsExpectedContracts()
    {
        BatchOperationProcessorService<string> service = Create();

        Assert.Multiple(() =>
        {
            Assert.That(service, Is.InstanceOf<IBatchOperationProcessorService<string>>());
            Assert.That(service, Is.InstanceOf<Microsoft.Extensions.Hosting.IHostedService>());
        });
    }

    [Test]
    public async Task Start_IsNonBlocking_AndServiceProcessesAfterwards()
    {
        BatchOperationProcessorService<string> service = Create(maxItems: 1);

        await service.StartAsync(CancellationToken.None).WithTimeout();
        await service.EnqueueAsync("a");

        await WaitForCallsAsync(_handler, 1);
    }

    [Test]
    public async Task Items_EnqueuedBeforeStart_AreProcessedOnceStarted()
    {
        BatchOperationProcessorService<string> service = Create(maxItems: 2);
        await service.EnqueueAsync(["a", "b", "c"]);

        await service.StartAsync(CancellationToken.None);

        await WaitForCallsAsync(_handler, 1);
        Assert.That(_handler.Batches[0], Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task Service_WithValueTypeItems_Works()
    {
        TestBatchHandler<int> intHandler = new();
        using BatchOperationProcessorService<int> service = new(
            intHandler,
            Options.Create(new BatchOperationHandlerOptions<int> { MaxItemPerBatch = 3 }),
            new TestLogger<BatchOperationProcessorService<int>>(),
            _time);
        await service.StartAsync(CancellationToken.None);

        await service.EnqueueAsync([1, 2, 3, 4]);
        await service.StopAsync(CancellationToken.None);

        Assert.That(intHandler.Batches, Is.EqualTo(new[] { new[] { 1, 2, 3 }, new[] { 4 } }));
    }

    #endregion Lifecycle

    #region Concurrency

    [Test]
    public async Task Concurrency_ManyProducers_AllItemsDeliveredOnceAndPerProducerOrderKept()
    {
        const int producers = 8;
        const int perProducer = 500;
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 50, maxQueueSize: 100);

        await Task.WhenAll(Enumerable.Range(0, producers).Select(p => Task.Run(async () =>
        {
            for (int i = 0; i < perProducer; i++)
            {
                await service.EnqueueAsync($"{p}:{i}");
            }
        }))).WithTimeout(TimeSpan.FromSeconds(30));
        await service.StopAsync(CancellationToken.None).WithTimeout(TimeSpan.FromSeconds(30));

        IReadOnlyList<string> all = _handler.AllItems;
        Assert.Multiple(() =>
        {
            Assert.That(all, Has.Count.EqualTo(producers * perProducer));
            Assert.That(all.Distinct().Count(), Is.EqualTo(producers * perProducer), "no duplicates");
            Assert.That(_handler.Batches.All(b => b.Length <= 50), Is.True);

            for (int p = 0; p < producers; p++)
            {
                int[] seq = [.. all.Where(x => x.StartsWith($"{p}:")).Select(x => int.Parse(x[(x.IndexOf(':') + 1)..]))];
                Assert.That(seq, Is.Ordered, $"producer {p} order");
            }
        });
    }

    [Test]
    public async Task Concurrency_ConcurrentSingleAndBulkEnqueues_LoseNothing()
    {
        BatchOperationProcessorService<string> service = await CreateAndStartAsync(maxItems: 25);
        ConcurrentBag<string> expected = [];

        Task singles = Task.Run(async () =>
        {
            for (int i = 0; i < 300; i++)
            {
                string item = $"s{i}";
                expected.Add(item);
                await service.EnqueueAsync(item);
            }
        });
        Task bulks = Task.Run(async () =>
        {
            for (int i = 0; i < 30; i++)
            {
                string[] chunk = [.. Enumerable.Range(0, 10).Select(j => $"b{i}-{j}")];
                foreach (string c in chunk)
                {
                    expected.Add(c);
                }

                await service.EnqueueAsync(chunk);
            }
        });

        await Task.WhenAll(singles, bulks);
        await service.StopAsync(CancellationToken.None).WithTimeout();

        Assert.That(_handler.AllItems, Is.EquivalentTo(expected));
    }

    #endregion Concurrency
}
