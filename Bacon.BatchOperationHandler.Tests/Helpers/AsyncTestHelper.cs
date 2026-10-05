using Microsoft.Extensions.Time.Testing;

namespace Bacon.BatchOperationHandler.Tests.Helpers;

internal static class AsyncTestHelper
{
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Real-time pause letting the background loop observe what was just enqueued
    /// (e.g. create/reset its fake-time timer) before the test advances the fake clock.
    /// </summary>
    public static Task SettleAsync(int milliseconds = 100) => Task.Delay(milliseconds);

    /// <summary>Polls (real time) until the condition holds, failing the test after the timeout.</summary>
    public static async Task WaitUntilAsync(Func<bool> condition, string description, TimeSpan? timeout = null)
    {
        DateTime limit = DateTime.UtcNow + (timeout ?? DefaultTimeout);

        while (!condition())
        {
            if (DateTime.UtcNow > limit)
            {
                Assert.Fail($"Timed out waiting for: {description}");
            }

            await Task.Delay(5);
        }
    }

    /// <summary>Advances the fake clock in steps (with a real-time yield between each) until the condition holds.</summary>
    public static async Task AdvanceUntilAsync(FakeTimeProvider timeProvider, Func<bool> condition, string description, TimeSpan step, TimeSpan? maxFakeTime = null)
    {
        TimeSpan advanced = TimeSpan.Zero;
        TimeSpan limit = maxFakeTime ?? TimeSpan.FromHours(1);
        DateTime realLimit = DateTime.UtcNow + DefaultTimeout * 3;

        while (!condition())
        {
            if (advanced > limit || DateTime.UtcNow > realLimit)
            {
                Assert.Fail($"Timed out waiting for: {description}");
            }

            timeProvider.Advance(step);
            advanced += step;
            await Task.Delay(10);
        }
    }

    public static async Task<T> WithTimeout<T>(this Task<T> task, TimeSpan? timeout = null)
    {
        return await task.WaitAsync(timeout ?? DefaultTimeout);
    }

    public static async Task WithTimeout(this Task task, TimeSpan? timeout = null)
    {
        await task.WaitAsync(timeout ?? DefaultTimeout);
    }
}
