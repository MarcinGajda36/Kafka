namespace Magj.Kafka;

using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

/// <summary>
/// Settings for method: <see cref="KafkaClient.AtLeastOnceAsync"/>.
/// </summary>
/// <param name="Topic">
/// The topic to subscribe to. A regex can be specified to subscribe to the set of
/// all matching topics (which is updated as topics are added / removed from the cluster). 
/// A regex must be front anchored to be recognized as a regex. e.g. ^myregex
/// </param>
/// <param name="BootstrapServers">Initial list of brokers as a CSV list of broker host or host:port.</param>
/// <param name="GroupId">Client group id string. All clients sharing the same group.id belong to the same group.</param>
public sealed record AtLeastOnceSettings(
    string Topic,
    string BootstrapServers = "localhost:9092",
    string GroupId = "")
{
    /// <summary>
    /// Number of times processor function can be called in parallel.
    /// Only change above 1 it if processing multiple messages at the same time is safe.
    /// 1 by default. -1 for unbounded. 
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = 1;

    /// <summary>
    /// Amount of messages that can be queued in memory for processing.
    /// Can be increased for potential perf improvements, or decreased to consume less memory.
    /// 4096 by default. -1 for unbounded.
    /// </summary>
    public int MaxBufferedMessages { get; init; } = 4096;

    /// <summary>
    /// How long we wait for message before we loop and try again.
    /// TimeSpan.FromSeconds(1) by default.
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
    /// Fatal kafka exception and Unhandled processor exceptions are still causing throw, so it may not be necessary to assign this.
    /// NullLogger.Instance by default.
    /// </summary>
    public ILogger Logger { get; init; } = NullLogger.Instance;

    /// <summary>
    /// Action to take when there is no initial offset in offset store or the desired offset is out of range.
    /// AutoOffsetReset.Earliest by default.
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; init; } = AutoOffsetReset.Earliest;
}

public static partial class KafkaClient
{
    /// <summary>
    /// At-least-once delivery guarantees no message is lost, but duplicates may occur during failures.
    /// </summary>
    /// <typeparam name="TKey">The Kafka message Key.</typeparam>
    /// <typeparam name="TValue">The Kafka message Value.</typeparam>
    /// <param name="settings">Settings for connecting to kafka and optionally for controlling processing details.</param>
    /// <param name="processor">Operation to do on each kafka message.</param>
    /// <param name="consumerConfigOptions">
    /// Action that allows configuring <see cref="ConsumerConfig"/>. 
    /// Invoked after <see cref="settings"/> are applied.
    /// EnableAutoOffsetStore will always be false to maintain the contract of method name 'AtLeastOnce'.</param>
    /// <param name="consumerBuilderOptions">Action that allows configuring <see cref="ConsumerBuilder{TKey, TValue}"/>.</param>
    /// <param name="cancellationToken">Token for cancelling processing.</param>
    /// <returns>Task that represents subscription and processing. Will throw on unhandled <see cref="processor"/> exceptions as well as on fatal <see cref="ConsumeException"/>.</returns>
    public static Task AtLeastOnceAsync<TKey, TValue>(
        AtLeastOnceSettings settings,
        Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> processor,
        Action<ConsumerConfig>? consumerConfigOptions = null,
        Action<ConsumerBuilder<TKey, TValue>>? consumerBuilderOptions = null,
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
        return AtLeastOnceCore(settings, processor, consumerConfigOptions, consumerBuilderOptions, cancellationToken);
    }

    /// <summary>
    /// At-least-once delivery guarantees no message is lost, but duplicates may occur during failures.
    /// </summary>
    /// <typeparam name="TKey">The Kafka message Key.</typeparam>
    /// <typeparam name="TValue">The Kafka message Value.</typeparam>
    /// <param name="settings">Settings for connecting to kafka and optionally for controlling processing details.</param>
    /// <param name="processor">Operation to do on each kafka message.</param>
    /// <param name="cancellationToken">Token for cancelling processing.</param>
    /// <returns>Task that represents subscription and processing. Will throw on unhandled <see cref="processor"/> exceptions as well as on fatal <see cref="ConsumeException"/>.</returns>
    public static Task AtLeastOnceAsync<TKey, TValue>(
        AtLeastOnceSettings settings,
        Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> processor,
        CancellationToken cancellationToken = default)
        => AtLeastOnceAsync(settings, processor, null, null, cancellationToken);

    private static async Task AtLeastOnceCore<TKey, TValue>(
        AtLeastOnceSettings settings,
        Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> processor,
        Action<ConsumerConfig>? consumerConfigOptions,
        Action<ConsumerBuilder<TKey, TValue>>? consumerBuilderOptions,
        CancellationToken cancellationToken)
    {
        ConsumerConfig configuration = new()
        {
            BootstrapServers = settings.BootstrapServers,
            AutoOffsetReset = settings.AutoOffsetReset,
            GroupId = settings.GroupId,
        };
        consumerConfigOptions?.Invoke(configuration);
        configuration.EnableAutoOffsetStore = false;

        var builder = new ConsumerBuilder<TKey, TValue>(configuration);
        consumerBuilderOptions?.Invoke(builder);
        using var client = builder.Build();
        await ConsumeAsync(client, processor, settings, cancellationToken);
    }

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
        while (cancellationToken.IsCancellationRequested is false)
        {
            try
            {
                var kafkaMessage = consumer.Consume(settings.ConsumeTimeout);
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