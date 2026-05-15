namespace Magj.Multithreating;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

public sealed class ReadExecuteRetire
{
    public readonly struct ParallelableStepOptions()
    {
        public TaskScheduler TaskScheduler { get; init; } = TaskScheduler.Default;
        public int MaxDegreeOfParallelism { get; init; } = 1;

        /// <summary>
        /// Maximum number of messages waiting in queue for processing.
        /// </summary>
        public int BoundedCapacity { get; init; } = 4096;
    }

    public readonly struct RetireStepOptions()
    {
        public TaskScheduler TaskScheduler { get; init; } = TaskScheduler.Default;
        /// <summary>
        /// Maximum number of messages waiting in queue for processing.
        /// </summary>
        public int BoundedCapacity { get; init; } = 4096;
    }

    public readonly struct Options()
    {
        /// <summary>
        /// Options for how 'read' function will be triggered.
        /// </summary>
        public ParallelableStepOptions Read { get; init; } = new();

        /// <summary>
        /// Options for how 'execute' function will be triggered.
        /// </summary>
        public ParallelableStepOptions Execute { get; init; } = new();

        /// <summary>
        /// Options for how 'retire' function will be triggered. 
        /// Only 'retire' function is suppose to produce side-effect, like saving to database.
        /// </summary>
        public RetireStepOptions Retire { get; init; } = new();
        public CancellationToken CancellationToken { get; init; } = CancellationToken.None;
    }

    private abstract record Message;
    private sealed record TriggerMessage<TTrigger>(TTrigger Trigger) : Message;
    private sealed record ValueMessage<TTrigger, TValue>(TTrigger Trigger, TValue Value) : Message;
    private sealed record DoneMessage() : Message
    {
        public static readonly DoneMessage Instance = new();
    }
    private sealed record ExceptionMessage(Exception Exception) : Message;

    /// <summary>
    /// Helper to deal create parallel and order preserving flow.
    /// The order is preserved only for 'retire' so 'retire' will see messages in exact same order they were triggered, 
    /// but for example if 'execute' has 'MaxDegreeOfParallelism > 1' then 'execute' can happen out of order. 
    /// This is intentional as only 'retire' is suppose to produce side-effect like saving to database.
    /// </summary>
    /// <typeparam name="TTrigger">Type of messages that will trigger rest of flow and allows for tracking what trigger caused what message.</typeparam>
    /// <typeparam name="TRead">Result of 'read' function.</typeparam>
    /// <typeparam name="TExecute">Result of 'execute' function.</typeparam>
    /// <param name="triggers">Potentially infinite collection of elements to process. Both finite and infinite collection work here.</param>
    /// <param name="read">Function doing first step of processing.</param>
    /// <param name="execute">Function doing second step of processing.</param>
    /// <param name="retire">Function doing last step of processing. To preserve order all side-effect like saving to Data Base should happen here.</param>
    /// <param name="options">Pass 'new()' for default options.</param>
    /// <returns>Task that signals completion of all work started by this method. If any step throws unhandled exception then processing stops and this task re-throws that exception.</returns>
    public static Task CreateAsync<TTrigger, TRead, TExecute>(
        IAsyncEnumerable<TTrigger> triggers,
        Func<TTrigger, CancellationToken, ValueTask<TRead>> read,
        Func<TTrigger, TRead, CancellationToken, ValueTask<TExecute>> execute,
        Func<TTrigger, TExecute, CancellationToken, ValueTask> retire,
        Options options)
    {
        ArgumentNullException.ThrowIfNull(triggers);
        ArgumentNullException.ThrowIfNull(read);
        ArgumentNullException.ThrowIfNull(execute);
        ArgumentNullException.ThrowIfNull(retire);

        return CoreAsync(triggers, read, execute, retire, options);

        static async Task CoreAsync(
            IAsyncEnumerable<TTrigger> triggers,
            Func<TTrigger, CancellationToken, ValueTask<TRead>> read,
            Func<TTrigger, TRead, CancellationToken, ValueTask<TExecute>> execute,
            Func<TTrigger, TExecute, CancellationToken, ValueTask> retire,
            Options options)
        {
            using var cancelationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(options.CancellationToken);
            var token = cancelationTokenSource.Token;

            var readBlock = CreateReadBlock(read, options.Read, token);
            var executeBlock = CreateExecuteBlock(execute, options.Execute, token);

            var retireCompletionSource = new TaskCompletionSource();
            using var retireCompletionSourceCancellation = token.Register(static (state, token) => ((TaskCompletionSource)state!).TrySetCanceled(token), retireCompletionSource);
            var retireBlock = CreateRetireBlock(retire, retireCompletionSource, options.Retire, token);

            var propagateCompletionOptions = new DataflowLinkOptions { PropagateCompletion = true };
            using var readToExecuteLink = readBlock.LinkTo(executeBlock, propagateCompletionOptions);
            using var executeToRetireLink = executeBlock.LinkTo(retireBlock, propagateCompletionOptions);

            var feed = FeedReadsAsync(triggers, readBlock, token);
            try
            {
                await retireCompletionSource.Task;
            }
            finally
            {
                await cancelationTokenSource.CancelAsync().ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                await Task.WhenAll(retireBlock.Completion, executeBlock.Completion, readBlock.Completion, feed).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
        }
    }

    private static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(IEnumerable<T> enumerable)
    {
        foreach (var item in enumerable)
        {
            yield return item;
        }
    }

    /// <summary>
    /// Helper to deal create parallel and order preserving flow.
    /// The order is preserved only for 'retire' so 'retire' will see messages in exact same order they were triggered, 
    /// but for example if 'execute' has 'MaxDegreeOfParallelism > 1' then 'execute' can happen out of order. 
    /// This is intentional as only 'retire' is suppose to produce side-effect like saving to database.
    /// </summary>
    /// <typeparam name="TTrigger">Type of messages that will trigger rest of flow and allows for tracking what trigger caused what message.</typeparam>
    /// <typeparam name="TRead">Result of 'read' function.</typeparam>
    /// <typeparam name="TExecute">Result of 'execute' function.</typeparam>
    /// <param name="triggers">Potentially infinite collection of elements to process. Both finite and infinite collection work here.</param>
    /// <param name="read">Function doing first step of processing.</param>
    /// <param name="execute">Function doing second step of processing.</param>
    /// <param name="retire">Function doing last step of processing. To preserve order all side-effect like saving to Data Base should happen here.</param>
    /// <param name="options">Pass 'new()' for default options.</param>
    /// <returns>Task that signals completion of all work started by this method. If any step throws unhandled exception then processing stops and this task re-throws that exception.</returns>
    public static Task CreateAsync<TTrigger, TRead, TExecute>(
        IEnumerable<TTrigger> triggers,
        Func<TTrigger, CancellationToken, ValueTask<TRead>> read,
        Func<TTrigger, TRead, CancellationToken, ValueTask<TExecute>> execute,
        Func<TTrigger, TExecute, CancellationToken, ValueTask> retire,
        Options options)
        => CreateAsync(
            ToAsyncEnumerable(triggers),
            read,
            execute,
            retire,
            options);

    private static TransformBlock<Message, Message> CreateReadBlock<TTrigger, TRead>(
        Func<TTrigger, CancellationToken, ValueTask<TRead>> read,
        ParallelableStepOptions readOptions,
        CancellationToken token)
        => new(
            async message =>
            {
                try
                {
                    return message switch
                    {
                        TriggerMessage<TTrigger>(var trigger)
                            => new ValueMessage<TTrigger, TRead>(trigger, await read(trigger, token)),
                        var otherMessage
                            => otherMessage,
                    };
                }
                catch (Exception exception)
                {
                    return new ExceptionMessage(exception);
                }
            },
            new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = readOptions.BoundedCapacity,
                MaxDegreeOfParallelism = readOptions.MaxDegreeOfParallelism,
                TaskScheduler = readOptions.TaskScheduler,
                CancellationToken = token,
            });

    private static TransformBlock<Message, Message> CreateExecuteBlock<TTrigger, TRead, TExecute>(
        Func<TTrigger, TRead, CancellationToken, ValueTask<TExecute>> execute,
        ParallelableStepOptions executeOptions,
        CancellationToken token)
        => new(
            async message =>
            {
                try
                {
                    return message switch
                    {
                        ValueMessage<TTrigger, TRead>(var trigger, var read)
                            => new ValueMessage<TTrigger, TExecute>(trigger, await execute(trigger, read, token)),
                        var otherMessage
                            => otherMessage,
                    };
                }
                catch (Exception exception)
                {
                    return new ExceptionMessage(exception);
                }
            },
            new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = executeOptions.BoundedCapacity,
                MaxDegreeOfParallelism = executeOptions.MaxDegreeOfParallelism,
                TaskScheduler = executeOptions.TaskScheduler,
                CancellationToken = token,
            });

    private static ActionBlock<Message> CreateRetireBlock<TTrigger, TExecute>(
        Func<TTrigger, TExecute, CancellationToken, ValueTask> retire,
        TaskCompletionSource retireCompletionSource,
        RetireStepOptions retireOptions,
        CancellationToken token)
        => new(
            async message =>
            {
                try
                {
                    if (retireCompletionSource.Task.IsCompleted)
                    {
                        return;
                    }

                    switch (message)
                    {
                        case ValueMessage<TTrigger, TExecute>(var trigger, var execute):
                            await retire(trigger, execute, token);
                            break;
                        case DoneMessage:
                            _ = retireCompletionSource.TrySetResult();
                            break;
                        case ExceptionMessage(var exception):
                            _ = retireCompletionSource.TrySetException(exception);
                            break;
                        default:
                            _ = retireCompletionSource.TrySetException(new ArgumentOutOfRangeException(nameof(message), message, "Unrecognized message type"));
                            break;
                    }
                }
                catch (Exception exception)
                {
                    _ = retireCompletionSource.TrySetException(exception);
                }
            },
            new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = retireOptions.BoundedCapacity,
                TaskScheduler = retireOptions.TaskScheduler,
                CancellationToken = token
            });

    private static async Task FeedReadsAsync<TTrigger>(
        IAsyncEnumerable<TTrigger> triggers,
        TransformBlock<Message, Message> read,
        CancellationToken token)
    {
        try
        {
            await foreach (var trigger in triggers.WithCancellation(token))
            {
                if ((await read.SendAsync(new TriggerMessage<TTrigger>(trigger), token)) is false)
                {
                    return;
                }
            }
        }
        catch (Exception exception)
        {
            _ = await read.SendAsync(new ExceptionMessage(exception), token);
        }
        finally
        {
            _ = await read.SendAsync(DoneMessage.Instance, token);
        }
    }
}

