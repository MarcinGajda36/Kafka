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
    private sealed record ValueMessage<TTrigger, TValue>(TTrigger Trigger, TValue Value) : Message;
    private sealed record ExceptionMessage(Exception Exception) : Message;

    public static Task Create<TTrigger, TRead, TExecute>(
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
            var reads = readBlock.Completion;
            var excecutes = executeBlock.Completion;
            var retires = retireBlock.Completion;

            var firstToFinish = await Task.WhenAny(feed, reads, excecutes, retires);
            await cancelationTokenSource.CancelAsync();
            await Task.WhenAll(firstToFinish, feed, reads, excecutes, retires);
        }
    }

    private static TransformBlock<TTrigger, Message> CreateReadBlock<TTrigger, TRead>(
        Func<TTrigger, CancellationToken, ValueTask<TRead>> read,
        SingleStepOptions readOptions,
        CancellationToken token)
        => new(
            async trigger =>
            {
                try
                {
                    return token.IsCancellationRequested
                        ? new ExceptionMessage(new OperationCanceledException(token))
                        : new ValueMessage<TTrigger, TRead>(
                            trigger,
                            await read(trigger, token));
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
            });

    private static TransformBlock<Message, Message> CreateExecuteBlock<TTrigger, TRead, TExecute>(
        Func<TTrigger, TRead, CancellationToken, ValueTask<TExecute>> execute,
        SingleStepOptions executeOptions,
        CancellationToken token)
        => new(
            message =>
            {
                return message switch
                {
                    ValueMessage<TTrigger, TRead>(var trigger, var value) => ExecuteAsync(trigger, value, execute, token),
                    var otherMessage => Task.FromResult(otherMessage),
                };

                static async Task<Message> ExecuteAsync(
                    TTrigger trigger,
                    TRead read,
                    Func<TTrigger, TRead, CancellationToken, ValueTask<TExecute>> execute,
                    CancellationToken token)
                {
                    try
                    {
                        return token.IsCancellationRequested
                            ? new ExceptionMessage(new OperationCanceledException(token))
                            : new ValueMessage<TTrigger, TExecute>(
                                trigger,
                                await execute(trigger, read, token));
                    }
                    catch (Exception exception)
                    {
                        return new ExceptionMessage(exception);
                    }
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
        TransformBlock<TTrigger, Message> readBlock,
        SingleStepOptions retireOptions,
        CancellationToken token)
        => new(
            message =>
            {
                return message switch
                {
                    ValueMessage<TTrigger, TExecute>(var trigger, var value) => RetireAsync(readBlock, trigger, value, retire, token),
                    ExceptionMessage(var exception) => FaultAsync(readBlock, exception),
                    var unknown => FaultAsync(readBlock, new ArgumentOutOfRangeException(nameof(message), unknown, "Unrecognized message type"))
                };

                static Task FaultAsync(IDataflowBlock firstBlock, Exception exception)
                {
                    firstBlock.Fault(exception);
                    return Task.FromException(exception);
                }

                static async Task RetireAsync(
                    IDataflowBlock firstBlock,
                    TTrigger trigger,
                    TExecute execute,
                    Func<TTrigger, TExecute, CancellationToken, ValueTask> retire,
                    CancellationToken token)
                {
                    if (token.IsCancellationRequested)
                    {
                        await FaultAsync(firstBlock, new OperationCanceledException(token));
                    }

                    try
                    {
                        await retire(trigger, execute, token);
                    }
                    catch (Exception exception)
                    {
                        await FaultAsync(firstBlock, exception);
                    }
                }
            },
            new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = retireOptions.BoundedCapacity,
                MaxDegreeOfParallelism = retireOptions.MaxDegreeOfParallelism,
                TaskScheduler = retireOptions.TaskScheduler,
            });

    private static async Task FeedReadsAsync<TTrigger>(
        IAsyncEnumerable<TTrigger> triggers,
        TransformBlock<TTrigger, Message> read,
        CancellationToken token)
    {
        await foreach (var trigger in triggers.WithCancellation(token))
        {
            if ((await read.SendAsync(trigger, token)) is false)
            {
                return;
            }
        }
    }
}

