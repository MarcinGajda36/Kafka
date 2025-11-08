namespace Magj.Kafka;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

public sealed record AtLeastOnceSettings(
    string Topic,
    string BootstrapServers,
    string GroupId)
{
    /// <summary>
    /// Number of times processor function can be called in parallel.
    /// Only change above 1 it if processing multiple messages at the same time is safe.
    /// 1 by default. -1 for unbounded. 
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = 1;

    /// <summary>
    /// Amount of messages that can wait in memory for processing.
    /// Can be increased for potential perf improvements, or decreased to consume less memory.
    /// 4096 by default. -1 for unbounded. 
    /// </summary>
    public int MaxBufferedMessages { get; init; } = 4096;

    /// <summary>
    /// How long we wait for message before we loop and try again.
    /// </summary>
    public TimeSpan ConsumeTimeout { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Scheduler used for receiving kafka messages and storing their offset after processing. 
    /// Leaving default is recommended.
    /// TaskScheduler.Default is default.
    /// </summary>
    public TaskScheduler ConsumerScheduler { get; init; } = TaskScheduler.Default;

    /// <summary>
    /// Scheduler used for processing kafka messages.
    /// TaskScheduler.Default is default.
    /// </summary>
    public TaskScheduler ProcessorScheduler { get; init; } = TaskScheduler.Default;

    /// <summary>
    /// Logger used to log Exceptions.
    /// Fatal kafka exception and Unhandled processor exceptions are still causing throw, so i may not be necessary to assign this.
    /// NullLogger.Instance by default.
    /// </summary>
    public ILogger Logger { get; init; } = NullLogger.Instance;
}

public sealed partial class KafkaClient
{
    public static Task AtLeastOnceAsync<TKey, TValue>(
        AtLeastOnceSettings settings,
        Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> processor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(settings);
        // TODO: maybe check settings.X in setters
        ArgumentOutOfRangeException.ThrowIfLessThan(settings.MaxDegreeOfParallelism, -1);
        ArgumentOutOfRangeException.ThrowIfLessThan(settings.MaxBufferedMessages, -1);
        ArgumentNullException.ThrowIfNull(settings.ConsumerScheduler);
        ArgumentNullException.ThrowIfNull(settings.ProcessorScheduler);
        ArgumentNullException.ThrowIfNull(settings.Logger);
        ArgumentNullException.ThrowIfNull(processor);
        return AtLeastOnceCore(settings, processor, cancellationToken);
    }

    private static async Task AtLeastOnceCore<TKey, TValue>(
        AtLeastOnceSettings settings,
        Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> processor,
        CancellationToken cancellationToken)
    {
        var configuration = AutoOffsetDisabledConfig(settings);
        using var client = new ConsumerBuilder<TKey, TValue>(configuration).Build();
        await ConsumeAsync(client, processor, settings, cancellationToken);
    }

    private static ConsumerConfig AutoOffsetDisabledConfig(AtLeastOnceSettings kafkaSettings)
        => new()
        {
            BootstrapServers = kafkaSettings.BootstrapServers,
            GroupId = kafkaSettings.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoOffsetStore = false,
        };

    private static async Task ConsumeAsync<TKey, TValue>(
        IConsumer<TKey, TValue> consumer,
        Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> processor,
        AtLeastOnceSettings settings,
        CancellationToken cancellationToken)
    {
        using var cancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cancellationToken = cancellationSource.Token;
        using var kafkaProcessor = new ProcessAndOffsetProcessor<TKey, TValue>(
            consumer,
            processor,
            settings,
            cancellationToken);

        var processorTask = kafkaProcessor.Completion;
        var consumerTask = Task.Factory.StartNew(
            () =>
            {
                consumer.Subscribe(settings.Topic);
                try
                {
                    ConsumeAndProcess(consumer, kafkaProcessor, settings, cancellationToken);
                }
                finally
                {
                    consumer.Close();
                }
            },
            cancellationToken,
            TaskCreationOptions.LongRunning,
            settings.ConsumerScheduler);

        var firstToFinish = await Task.WhenAny(consumerTask, processorTask);
        await cancellationSource.CancelAsync();
        await Task.WhenAll(firstToFinish, consumerTask, processorTask);
    }

    private static void ConsumeAndProcess<TKey, TValue>(
        IConsumer<TKey, TValue> consumer,
        ProcessAndOffsetProcessor<TKey, TValue> kafkaProcessor,
        AtLeastOnceSettings settings,
        CancellationToken cancellationToken)
    {
        var logger = settings.Logger;
        object?[] loggerParams = [settings.Topic, settings.GroupId];
        var consumeTimeout = settings.ConsumeTimeout;
        while (cancellationToken.IsCancellationRequested is false)
        {
            try
            {
                var kafkaMessage = consumer.Consume(consumeTimeout);
                if (kafkaMessage != null)
                {
                    if (kafkaProcessor.Enqueue(kafkaMessage) is false)
                    {
                        return;
                    }
                }
            }
            catch (ConsumeException ex)
            {
                // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                if (ex.Error.IsFatal)
                {
                    logger.LogError(
                        ex,
                        "Fatal exception during consuming from topic: {Topic}, groupId: {GroupId}. Closing consumption.",
                        loggerParams);
                    kafkaProcessor.Complete();
                    throw;
                }
                else
                {
                    logger.LogWarning(
                        ex,
                        "Non fatal exception during consuming from topic: {Topic}, groupId: {GroupId}. Trying again.",
                        loggerParams);
                }
            }
            catch (OperationCanceledException ex)
            {
                logger.LogInformation(
                    ex,
                    "Canceled exception during consuming from topic: {Topic}, groupId: {GroupId}. Closing consumption.",
                    loggerParams);
                kafkaProcessor.Complete();
                throw;
            }
            catch (Exception ex)
            {
                logger.LogError(
                    ex,
                    "Unknown exception during consuming from topic: {Topic}, groupId: {GroupId}. Closing consumption.",
                    loggerParams);
                kafkaProcessor.Complete();
                throw;
            }
        }
    }
}
