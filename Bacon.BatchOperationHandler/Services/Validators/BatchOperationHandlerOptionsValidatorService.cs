using Bacon.BatchOperationHandler.Models;
using Microsoft.Extensions.Options;

namespace Bacon.BatchOperationHandler.Services.Validators;

internal sealed class BatchOperationHandlerOptionsValidatorService<TItem> : IValidateOptions<BatchOperationHandlerOptions<TItem>>
{
    #region Properties

    private const ushort _minBatchItemCount = 1;
    private const ushort _maxBatchItemCount = 1_000;
    private const int _maxQueueSize = 100_000;
    private static readonly TimeSpan _minTimerInterval = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan _maxTimerInterval = TimeSpan.FromMinutes(5);

    #endregion Properties

    public ValidateOptionsResult Validate(string? name, BatchOperationHandlerOptions<TItem> batchOperationHandlerOptions)
    {
        List<string> errors = [];

        if (batchOperationHandlerOptions.MaxItemPerBatch is < _minBatchItemCount or > _maxBatchItemCount)
        {
            errors.Add($"{nameof(batchOperationHandlerOptions.MaxItemPerBatch)} must be between {_minBatchItemCount} and {_maxBatchItemCount}.");
        }

        if (batchOperationHandlerOptions.SlidingInterval < _minTimerInterval || batchOperationHandlerOptions.SlidingInterval > _maxTimerInterval)
        {
            errors.Add($"{nameof(batchOperationHandlerOptions.SlidingInterval)} must be between {_minTimerInterval.TotalMilliseconds} milliseconds and {_maxTimerInterval.TotalSeconds} seconds.");
        }

        if (batchOperationHandlerOptions.MaxQueueSize is int maxQueueSize && (maxQueueSize < batchOperationHandlerOptions.MaxItemPerBatch || maxQueueSize > _maxQueueSize))
        {
            errors.Add($"{nameof(batchOperationHandlerOptions.MaxQueueSize)} must be between {nameof(batchOperationHandlerOptions.MaxItemPerBatch)} and {_maxQueueSize}.");
        }

        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}