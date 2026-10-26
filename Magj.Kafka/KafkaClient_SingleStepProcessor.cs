namespace Magj.Kafka;

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

public partial class KafkaClient
{
    private sealed class SingleStepProcessor<TKey, TValue> : IDisposable
    {
        private readonly TransformBlock<ConsumeResult<TKey, TValue>, ConsumeResult<TKey, TValue>> processingBlock;
        private readonly IDisposable processingOffsetLink;

        internal readonly Task Completion;

        public SingleStepProcessor(
            IConsumer<TKey, TValue> consumer,
            Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> processor,
            AtLeastOnceSettings settings,
            CancellationToken cancellationToken)
        {
            processingBlock = CreateProcessingBlock(processor, settings, cancellationToken);
            var offsetBlock = CreateOffsetBlock(consumer, settings);
            processingOffsetLink = processingBlock.LinkTo(
                offsetBlock,
                propagateCompletionLinkOptions);
            Completion = Task.WhenAll(processingBlock.Completion, offsetBlock.Completion);
        }

        public bool Enqueue(ConsumeResult<TKey, TValue> kafkaMessage)
            => processingBlock.Post(kafkaMessage);

        public void Complete()
            => processingBlock.Complete();

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
                        _processingError(settings.Logger, settings.Topic, settings.GroupId, ex);
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
            => new(
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
                            _fatalStoreOffsetError(settings.Logger, settings.Topic, settings.GroupId, ex);
                            throw;
                        }
                        else
                        {
                            _nonFatalStoreOffsetWarning(settings.Logger, settings.Topic, settings.GroupId, ex);
                        }
                    }
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = settings.MaxBufferedMessages,
                    TaskScheduler = settings.ConsumerScheduler,
                    SingleProducerConstrained = true,
                });

        public void Dispose()
            => processingOffsetLink.Dispose();

        private static readonly Action<ILogger, string, string, Exception?> _processingError =
            LoggerMessage.Define<string, string>(
                LogLevel.Error,
                new EventId(5, nameof(_processingError)),
                "Unhandled exception during processing from topic: {Topic}, groupId: {GroupId}. Closing processing.");

        private static readonly Action<ILogger, string, string, Exception?> _fatalStoreOffsetError =
            LoggerMessage.Define<string, string>(
                LogLevel.Error,
                new EventId(6, nameof(_fatalStoreOffsetError)),
                "Fatal exception during StoreOffset from topic: {Topic}, groupId: {GroupId}. Closing consumption.");

        private static readonly Action<ILogger, string, string, Exception?> _nonFatalStoreOffsetWarning =
            LoggerMessage.Define<string, string>(
                LogLevel.Warning,
                new EventId(7, nameof(_nonFatalStoreOffsetWarning)),
                "Non fatal exception during StoreOffset from topic: {Topic}, groupId: {GroupId}. Continuing.");
    }
}
