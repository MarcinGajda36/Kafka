namespace Magj.Kafka;

using System;
using System.Threading.Tasks;
using Confluent.Kafka;

// TODO: add some docs
internal sealed record AtLeastOnceMultiStepSettings(
    string Topic,
    string BootstrapServers,
    string GroupId)
{
    /// <summary>
    /// How long we wait for message before we loop and try again.
    /// </summary>
    public TimeSpan ConsumeTimeout { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Action to take when there is no initial offset in offset store or the desired offset is out of range.
    /// AutoOffsetReset.Earliest by default.
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; init; } = AutoOffsetReset.Earliest;

    public AtLeastOnceStepSettings ReadSettings { get; init; } = new();
    public AtLeastOnceStepSettings ExecuteSettings { get; init; } = new();
    public AtLeastOnceRetireSettings RetireSettings { get; init; } = new();
}

internal sealed record AtLeastOnceStepSettings()
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
    /// Scheduler used for processing kafka messages.
    /// TaskScheduler.Default by default.
    /// </summary>
    public TaskScheduler StepScheduler { get; init; } = TaskScheduler.Default;
}

internal sealed record AtLeastOnceRetireSettings()
{
    /// <summary>
    /// Amount of messages that can wait in memory for processing.
    /// Can be increased for potential perf improvements, or decreased to consume less memory.
    /// 4096 by default. -1 for unbounded.
    /// </summary>
    public int MaxBufferedMessages { get; init; } = 4096;

    /// <summary>
    /// Scheduler used for processing kafka messages.
    /// TaskScheduler.Default by default.
    /// </summary>
    public TaskScheduler StepScheduler { get; init; } = TaskScheduler.Default;
}

public partial class KafkaClient
{
    ///// <summary>
    ///// At-least-once delivery guarantees no message is lost, but duplicates may occur during failures.
    ///// </summary>
    ///// <typeparam name="TKey">The Kafka message Key.</typeparam>
    ///// <typeparam name="TValue">The Kafka message Value.</typeparam>
    ///// <param name="settings">Settings for connecting to kafka and optionally for controlling processing details.</param>
    ///// <param name="processor">Operation to do on each kafka message.</param>
    ///// <param name="consumerConfigOptions">
    ///// Action that allows configuring <see cref="ConsumerConfig"/>. 
    ///// Invoked after <see cref="settings"/> are applied.
    ///// EnableAutoOffsetStore will always be false to maintain the contract of method name 'AtLeastOnce'.</param>
    ///// <param name="consumerBuilderOptions">Action that allows configuring <see cref="ConsumerBuilder{TKey, TValue}"/>.</param>
    ///// <param name="cancellationToken">Token for cancelling processing.</param>
    ///// <returns>Task that represents subscription and processing. Will throw on unhandled <see cref="processor"/> exceptions as well as on fatal <see cref="ConsumeException"/>.</returns>
    //public static Task AtLeastOnceMultiStepAsync<TKey, TValue>(
    //    AtLeastOnceSettings settings,
    //    Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> processor,
    //    Action<ConsumerConfig>? consumerConfigOptions = null,
    //    Action<ConsumerBuilder<TKey, TValue>>? consumerBuilderOptions = null,
    //    CancellationToken cancellationToken = default)
    //{
    //    ArgumentNullException.ThrowIfNull(settings);
    //    // TODO: maybe check settings.X in setters
    //    ArgumentOutOfRangeException.ThrowIfLessThan(settings.MaxDegreeOfParallelism, -1);
    //    ArgumentOutOfRangeException.ThrowIfLessThan(settings.MaxBufferedMessages, -1);
    //    ArgumentNullException.ThrowIfNull(settings.ConsumerScheduler);
    //    ArgumentNullException.ThrowIfNull(settings.ProcessorScheduler);
    //    ArgumentNullException.ThrowIfNull(settings.Logger);
    //    ArgumentNullException.ThrowIfNull(processor);
    //    return AtLeastOnceMultiStepCore(settings, processor, consumerConfigOptions, consumerBuilderOptions, cancellationToken);
    //}

    ///// <summary>
    ///// At-least-once delivery guarantees no message is lost, but duplicates may occur during failures.
    ///// </summary>
    ///// <typeparam name="TKey">The Kafka message Key.</typeparam>
    ///// <typeparam name="TValue">The Kafka message Value.</typeparam>
    ///// <param name="settings">Settings for connecting to kafka and optionally for controlling processing details.</param>
    ///// <param name="processor">Operation to do on each kafka message.</param>
    ///// <param name="cancellationToken">Token for cancelling processing.</param>
    ///// <returns>Task that represents subscription and processing. Will throw on unhandled <see cref="processor"/> exceptions as well as on fatal <see cref="ConsumeException"/>.</returns>
    //public static Task AtLeastOnceMultiStepAsync<TKey, TValue>(
    //    AtLeastOnceSettings settings,
    //    Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> processor,
    //    CancellationToken cancellationToken = default)
    //    => AtLeastOnceAsync(settings, processor, null, null, cancellationToken);

    //private static async Task AtLeastOnceMultiStepCore<TKey, TValue>(
    //    AtLeastOnceSettings settings,
    //    Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> processor,
    //    Action<ConsumerConfig>? consumerConfigOptions,
    //    Action<ConsumerBuilder<TKey, TValue>>? consumerBuilderOptions,
    //    CancellationToken cancellationToken)
    //{
    //    var configuration = new ConsumerConfig()
    //    {
    //        BootstrapServers = settings.BootstrapServers,
    //        AutoOffsetReset = settings.AutoOffsetReset,
    //        GroupId = settings.GroupId,
    //    };
    //    consumerConfigOptions?.Invoke(configuration);
    //    configuration.EnableAutoOffsetStore = false;

    //    var builder = new ConsumerBuilder<TKey, TValue>(configuration);
    //    consumerBuilderOptions?.Invoke(builder);
    //    using var client = builder.Build();
    //    await ConsumeAsync(client, processor, settings, cancellationToken);
    //}

    //private static async Task ConsumeAsync<TKey, TValue>(
    //    IConsumer<TKey, TValue> consumer,
    //    Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> processor,
    //    AtLeastOnceSettings settings,
    //    CancellationToken cancellationToken)
    //{
    //    using var cancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    //    cancellationToken = cancellationSource.Token;
    //    using var kafkaProcessor = new SingleStepProcessor<TKey, TValue>(
    //        consumer,
    //        processor,
    //        settings,
    //        cancellationToken);

    //    var processorTask = kafkaProcessor.Completion;
    //    var consumerTask = Task.Factory.StartNew(
    //        () =>
    //        {
    //            consumer.Subscribe(settings.Topic);
    //            try
    //            {
    //                ConsumeAndProcess(consumer, kafkaProcessor, settings, cancellationToken);
    //            }
    //            finally
    //            {
    //                consumer.Close();
    //            }
    //        },
    //        cancellationToken,
    //        TaskCreationOptions.LongRunning,
    //        settings.ConsumerScheduler);

    //    var firstToFinish = await Task.WhenAny(consumerTask, processorTask);
    //    await cancellationSource.CancelAsync();
    //    await Task.WhenAll(firstToFinish, consumerTask, processorTask);
    //}

    //private static void ConsumeAndProcess<TKey, TValue>(
    //    IConsumer<TKey, TValue> consumer,
    //    MultiStepProcessor<TKey, TValue> kafkaProcessor,
    //    AtLeastOnceSettings settings,
    //    CancellationToken cancellationToken)
    //{
    //    while (cancellationToken.IsCancellationRequested is false)
    //    {
    //        try
    //        {
    //            var kafkaMessage = consumer.Consume(settings.ConsumeTimeout);
    //            if (kafkaMessage != null)
    //            {
    //                if (kafkaProcessor.Enqueue(kafkaMessage) is false)
    //                {
    //                    return;
    //                }
    //            }
    //        }
    //        catch (Exception ex)
    //        {

    //        }
    //    }
    //}
}