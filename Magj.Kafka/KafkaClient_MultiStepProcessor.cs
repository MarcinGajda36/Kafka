namespace Magj.Kafka;

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

public partial class KafkaClient
{
    private sealed class MultiStepProcessor<TKey, TValue, TRead, TExecute> : IDisposable
    {
        private sealed class Done() { public static readonly Done Instance = new(); }

        private readonly struct StepMessage<TTrigger>(object? doneOrExceptionOrKafkaMessage, TValue value)
        {
            public readonly object? DoneOrExceptionOrKafkaMessage = doneOrExceptionOrKafkaMessage;
            public readonly TValue Value = value;
        }

        private readonly TransformBlock<ConsumeResult<TKey, TValue>, StepMessage<TRead>> readBlock;
        private readonly IDisposable links;

        internal readonly Task Completion;

        public MultiStepProcessor(
            IConsumer<TKey, TValue> consumer,
            Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask<TRead>> read,
            Func<ConsumeResult<TKey, TValue>, TRead, CancellationToken, ValueTask<TExecute>> execute,
            Func<ConsumeResult<TKey, TValue>, TExecute, CancellationToken, ValueTask> retire,
            AtLeastOnceSettings settings,
            CancellationToken cancellationToken)
        {
            //readBlock = CreateProcessingBlock(processor, settings, cancellationToken);
            //var offsetBlock = CreateOffsetBlock(consumer, settings);
            //links = readBlock.LinkTo(
            //    offsetBlock,
            //    new DataflowLinkOptions { PropagateCompletion = true });
            //Completion = offsetBlock.Completion;
        }

        public bool Enqueue(ConsumeResult<TKey, TValue> kafkaMessage)
            => readBlock.Post(kafkaMessage);

        public void Complete()
            => readBlock.Complete();

        private static TransformBlock<ConsumeResult<TKey, TValue>, ConsumeResult<TKey, TValue>> CreateProcessingBlock(
            Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> processor,
            AtLeastOnceSettings settings,
            CancellationToken cancellationToken)
            => new(
                async result =>
                {
                    try
                    {
                        await processor(result, cancellationToken);
                        return result;
                    }
                    catch (Exception ex)
                    {
                        settings.Logger.LogError(
                            ex,
                            "Unhandled exception during processing from topic: {Topic}, groupId: {GroupId}. Closing processing.",
                            settings.Topic,
                            settings.GroupId);
                        throw;
                    }
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = settings.MaxBufferedMessages,
                    MaxDegreeOfParallelism = settings.MaxDegreeOfParallelism,
                    TaskScheduler = settings.ProcessorScheduler,
                    CancellationToken = cancellationToken,
                });

        private static ActionBlock<ConsumeResult<TKey, TValue>> CreateOffsetBlock(
            IConsumer<TKey, TValue> consumer,
            AtLeastOnceSettings settings)
        {
            var logger = settings.Logger;
            object?[] loggerParams = [settings.Topic, settings.GroupId];
            return new(
                result =>
                {
                    try
                    {
                        consumer.StoreOffset(result);
                    }
                    catch (KafkaException ex)
                    {
                        // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                        if (ex.Error.IsFatal)
                        {
                            logger.LogError(
                                ex,
                                "Fatal exception during StoreOffset from topic: {Topic}, groupId: {GroupId}. Closing consumption.",
                                loggerParams);
                            throw;
                        }
                        else
                        {
                            logger.LogWarning(
                                ex,
                                "Non fatal exception during StoreOffset from topic: {Topic}, groupId: {GroupId}. Continuing.",
                                loggerParams);
                        }
                    }
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = settings.MaxBufferedMessages,
                    TaskScheduler = settings.ConsumerScheduler,
                    SingleProducerConstrained = true,
                });
        }

        public void Dispose()
            => links.Dispose();
    }
}
