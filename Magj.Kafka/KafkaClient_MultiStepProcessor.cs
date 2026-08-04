namespace Magj.Kafka;

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;

public partial class KafkaClient
{
    private sealed class MultiStepProcessor<TKey, TValue, TRead, TExecute> : IDisposable
    {
        private sealed class Done() { public static readonly Done Instance = new(); }

        private readonly struct StepMessage<TStepResult>(object? doneOrExceptionOrKafkaMessage, TStepResult stepResult)
        {
            public readonly object? DoneOrExceptionOrKafkaMessage = doneOrExceptionOrKafkaMessage;
            public readonly TStepResult StepResult = stepResult;
        }

        private sealed class MultiDispose(IDisposable[] toDispose) : IDisposable
        {
            public void Dispose() => Array.ForEach(toDispose, disposable => disposable.Dispose());
        }

        private readonly TransformBlock<object, StepMessage<TRead>> readBlock;
        private readonly MultiDispose links;

        public MultiStepProcessor(
            IConsumer<TKey, TValue> consumer,
            Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask<TRead>> read,
            Func<ConsumeResult<TKey, TValue>, TRead, CancellationToken, ValueTask<TExecute>> execute,
            Func<ConsumeResult<TKey, TValue>, TExecute, CancellationToken, ValueTask> retire,
            AtLeastOnceMultiStepSettings settings,
            TaskCompletionSource retireCompletionSource,
            CancellationToken cancellationToken)
        {
            readBlock = CreateReadBlock(read, settings.ReadSettings, cancellationToken);
            var executeBlock = CreateExecuteBlock(execute, settings.ExecuteSettings, cancellationToken);
            var retireBlock = CreateRetireBlock(retireCompletionSource, retire, consumer, settings.RetireSettings, cancellationToken);
            var readExecuteLink = readBlock.LinkTo(
                executeBlock,
                new DataflowLinkOptions { PropagateCompletion = true });
            var executeRetireLink = executeBlock.LinkTo(
                retireBlock,
                new DataflowLinkOptions { PropagateCompletion = true });
            links = new MultiDispose([readExecuteLink, executeRetireLink]);
        }

        public bool Enqueue(object kafkaMessage)
            => readBlock.Post(kafkaMessage);

        public void Complete()
            => readBlock.Complete();

        private static StepMessage<StepMessage> UnexpectedMessage<StepMessage>(object message)
            => new(new ArgumentOutOfRangeException(nameof(message), message, "Unexpected message."), default!);

        private static TransformBlock<object, StepMessage<TRead>> CreateReadBlock(
            Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask<TRead>> read,
            AtLeastOnceStepSettings settings,
            CancellationToken cancellationToken)
            => new(
                async fromKafka =>
                {
                    try
                    {
                        return fromKafka switch
                        {
                            ConsumeResult<TKey, TValue> kafkaMessage => new StepMessage<TRead>(kafkaMessage, await read(kafkaMessage, cancellationToken)),
                            Done done => new StepMessage<TRead>(done, default!),
                            Exception exception => new StepMessage<TRead>(exception, default!),
                            var unexpected => UnexpectedMessage<TRead>(unexpected),
                        };
                    }
                    catch (Exception ex)
                    {
                        return new StepMessage<TRead>(ex, default!);
                    }
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = settings.MaxBufferedMessages,
                    MaxDegreeOfParallelism = settings.MaxDegreeOfParallelism,
                    TaskScheduler = settings.StepScheduler,
                    CancellationToken = cancellationToken,
                });

        private static TransformBlock<StepMessage<TRead>, StepMessage<TExecute>> CreateExecuteBlock(
            Func<ConsumeResult<TKey, TValue>, TRead, CancellationToken, ValueTask<TExecute>> execute,
            AtLeastOnceStepSettings settings,
            CancellationToken cancellationToken)
            => new(
                async fromRead =>
                {
                    try
                    {
                        return fromRead switch
                        {
                            { DoneOrExceptionOrKafkaMessage: ConsumeResult<TKey, TValue> kafkaMessage, StepResult: var read }
                                => new StepMessage<TExecute>(kafkaMessage, await execute(kafkaMessage, read, cancellationToken)),
                            { DoneOrExceptionOrKafkaMessage: Done done }
                                => new StepMessage<TExecute>(done, default!),
                            { DoneOrExceptionOrKafkaMessage: Exception exception }
                                => new StepMessage<TExecute>(exception, default!),
                            var unexpected => UnexpectedMessage<TExecute>(unexpected),
                        };
                    }
                    catch (Exception ex)
                    {
                        return new StepMessage<TExecute>(ex, default!);
                    }
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = settings.MaxBufferedMessages,
                    MaxDegreeOfParallelism = settings.MaxDegreeOfParallelism,
                    TaskScheduler = settings.StepScheduler,
                    CancellationToken = cancellationToken,
                });

        private static ActionBlock<StepMessage<TExecute>> CreateRetireBlock(
            TaskCompletionSource retireCompletionSource,
            Func<ConsumeResult<TKey, TValue>, TExecute, CancellationToken, ValueTask> retire,
            IConsumer<TKey, TValue> consumer,
            AtLeastOnceRetireSettings settings,
            CancellationToken cancellationToken)
            => new(
                async fromExecute =>
                {
                    try
                    {
                        if (retireCompletionSource.Task.IsCompleted)
                        {
                            return;
                        }

                        switch (fromExecute)
                        {
                            case { DoneOrExceptionOrKafkaMessage: ConsumeResult<TKey, TValue> kafkaMessage, StepResult: var execute }:
                                await retire(kafkaMessage, execute, cancellationToken);
                                consumer.StoreOffset(kafkaMessage);
                                break;
                            case { DoneOrExceptionOrKafkaMessage: Done }:
                                _ = retireCompletionSource.TrySetResult();
                                break;
                            case { DoneOrExceptionOrKafkaMessage: Exception exception }:
                                _ = retireCompletionSource.TrySetException(exception);
                                break;
                            default:
                                _ = retireCompletionSource.TrySetException(new ArgumentOutOfRangeException(nameof(fromExecute), fromExecute, "Unexpected message."));
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
                    BoundedCapacity = settings.MaxBufferedMessages,
                    TaskScheduler = settings.StepScheduler,
                    CancellationToken = cancellationToken,
                });

        public void Dispose()
            => links.Dispose();
    }
}
