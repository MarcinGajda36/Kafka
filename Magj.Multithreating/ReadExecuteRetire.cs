namespace Magj.Multithreating;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

public class ReadExecuteRetire
{
    public readonly struct SingleStepOptions()
    {
        public TaskScheduler TaskScheduler { get; init; } = TaskScheduler.Default;
        public int MaxDegreeOfParallelism { get; init; } = 1;
        public int BoundedCapacity { get; init; } = 4096;
    }
    public readonly struct Options()
    {
        public SingleStepOptions Read { get; init; } = new();
        public SingleStepOptions Execute { get; init; } = new();
        public SingleStepOptions Retire { get; init; } = new();
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

        return Core(triggers, read, execute, retire, options);

        static async Task Core(
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
            var retireBlock = CreateRetireBlock(retire, readBlock, options.Retire, token);

            var propagateCompletionOptions = new DataflowLinkOptions { PropagateCompletion = true };
            using var readToExecuteLink = readBlock.LinkTo(executeBlock, propagateCompletionOptions);
            using var executeToRetireLink = executeBlock.LinkTo(retireBlock, propagateCompletionOptions);

            var feed = FeedReadsAsync(triggers, readBlock, token);
            var retires = retireBlock.Completion;

            await retires;
            await cancelationTokenSource.CancelAsync();
            await Task.WhenAll(retires, executeBlock.Completion, readBlock.Completion, feed);
        }
    }

    private static TransformBlock<Message, Message> CreateReadBlock<TTrigger, TRead>(
        Func<TTrigger, CancellationToken, ValueTask<TRead>> read,
        SingleStepOptions readOptions,
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
        SingleStepOptions executeOptions,
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
            });

    private static ActionBlock<Message> CreateRetireBlock<TTrigger, TExecute>(
        Func<TTrigger, TExecute, CancellationToken, ValueTask> retire,
        IDataflowBlock readBlock,
        SingleStepOptions retireOptions,
        CancellationToken token)
    {
        var isDone = false;
        return new(
            async message =>
            {
                try
                {
                    if (isDone)
                    {
                        return;
                    }
                    // TODO: float exception with TaskCompletionSource to avoid AgregateExe(AgregateExe(AgregateExe(...)));
                    switch (message)
                    {
                        case ValueMessage<TTrigger, TExecute>(var trigger, var execute):
                            await retire(trigger, execute, token);
                            break;
                        case DoneMessage:
                            readBlock.Complete();
                            isDone = true;
                            break;
                        case ExceptionMessage(var exception):
                            readBlock.Fault(exception);
                            isDone = true;
                            break;
                        default:
                            readBlock.Fault(new ArgumentOutOfRangeException(nameof(message), message, "Unrecognized message type"));
                            isDone = true;
                            break;
                    }
                }
                catch (Exception exception)
                {
                    readBlock.Fault(exception);
                    isDone = true;
                }
            },
            new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = retireOptions.BoundedCapacity,
                MaxDegreeOfParallelism = retireOptions.MaxDegreeOfParallelism,
                TaskScheduler = retireOptions.TaskScheduler,
            });
    }

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

