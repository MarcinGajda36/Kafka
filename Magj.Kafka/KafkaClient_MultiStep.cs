namespace Magj.Kafka;

using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

/// <summary>
/// Settings for method: <see cref="KafkaClient.AtLeastOnceMultiStepAsync"/>.
/// </summary>
/// <param name="Topic">
/// The topic to subscribe to. A regex can be specified to subscribe to the set of
/// all matching topics (which is updated as topics are added / removed from the cluster). 
/// A regex must be front anchored to be recognized as a regex. e.g. ^myregex
/// </param>
/// <param name="BootstrapServers">Initial list of brokers as a CSV list of broker host or host:port.</param>
/// <param name="GroupId">Client group id string. All clients sharing the same group.id belong to the same group.</param>
public sealed record AtLeastOnceMultiStepSettings(
    string Topic,
    string BootstrapServers = "localhost:9092",
    string GroupId = "")
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

    /// <summary>
    /// Settings for controlling how 'read' function will run.
    /// </summary>
    public AtLeastOnceStepSettings ReadSettings { get; init; } = new();

    /// <summary>
    /// Settings for controlling how 'execute' function will run.
    /// </summary>
    public AtLeastOnceStepSettings ExecuteSettings { get; init; } = new();

    /// <summary>
    /// Settings for controlling how 'retire' function will run.
    /// </summary>
    public AtLeastOnceRetireSettings RetireSettings { get; init; } = new();
}

public sealed record AtLeastOnceStepSettings()
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

public sealed record AtLeastOnceRetireSettings()
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
    /// <summary>
    /// At-least-once delivery guarantees no message is lost, but duplicates may occur during failures.
    /// Allows for different steps to happen concurrently with their own MaxDegreeOfParallelism.
    /// </summary>
    /// <typeparam name="TKafkaKey">The Kafka message Key.</typeparam>
    /// <typeparam name="TKafkaValue">The Kafka message Value.</typeparam>
    /// <param name="settings">Settings for connecting to kafka and optionally for controlling processing details.</param>
    /// <param name="read">First operation to do on each kafka message.</param>
    /// <param name="execute">Second operation to do on each kafka message.</param>
    /// <param name="retire">
    /// Final operation to do on each kafka message. 
    /// To preserve order all side-effects, like saving to database, should happen here.
    /// Triggers in exact same order as messages arrived from kafka with MaxDegreeOfParallelism set to 1.
    /// </param>
    /// <param name="consumerConfigOptions">
    /// Action that allows configuring <see cref="ConsumerConfig"/>. 
    /// Invoked after <see cref="settings"/> are applied.
    /// EnableAutoOffsetStore will always be false to maintain the contract of method name 'AtLeastOnce'.</param>
    /// <param name="consumerBuilderOptions">Action that allows configuring <see cref="ConsumerBuilder{TKafkaKey, TKafkaValue}"/>.</param>
    /// <param name="cancellationToken">Token for cancelling processing.</param>
    /// <returns>Task that represents subscription and processing. Will throw on unhandled exceptions.</returns>
    public static Task AtLeastOnceMultiStepAsync<TKafkaKey, TKafkaValue, TRead, TExecute>(
        AtLeastOnceMultiStepSettings settings,
        Func<ConsumeResult<TKafkaKey, TKafkaValue>, CancellationToken, ValueTask<TRead>> read,
        Func<ConsumeResult<TKafkaKey, TKafkaValue>, TRead, CancellationToken, ValueTask<TExecute>> execute,
        Func<ConsumeResult<TKafkaKey, TKafkaValue>, TExecute, CancellationToken, ValueTask> retire,
        Action<ConsumerConfig>? consumerConfigOptions = null,
        Action<ConsumerBuilder<TKafkaKey, TKafkaValue>>? consumerBuilderOptions = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(read);
        ArgumentNullException.ThrowIfNull(execute);
        ArgumentNullException.ThrowIfNull(retire);
        return AtLeastOnceMultiStepCore(settings, read, execute, retire, consumerConfigOptions, consumerBuilderOptions, cancellationToken);
    }

    private static async Task AtLeastOnceMultiStepCore<TKafkaKey, TKafkaValue, TRead, TExecute>(
        AtLeastOnceMultiStepSettings settings,
        Func<ConsumeResult<TKafkaKey, TKafkaValue>, CancellationToken, ValueTask<TRead>> read,
        Func<ConsumeResult<TKafkaKey, TKafkaValue>, TRead, CancellationToken, ValueTask<TExecute>> execute,
        Func<ConsumeResult<TKafkaKey, TKafkaValue>, TExecute, CancellationToken, ValueTask> retire,
        Action<ConsumerConfig>? consumerConfigOptions,
        Action<ConsumerBuilder<TKafkaKey, TKafkaValue>>? consumerBuilderOptions,
        CancellationToken cancellationToken)
    {
        var configuration = new ConsumerConfig()
        {
            BootstrapServers = settings.BootstrapServers,
            AutoOffsetReset = settings.AutoOffsetReset,
            GroupId = settings.GroupId,
        };
        consumerConfigOptions?.Invoke(configuration);
        configuration.EnableAutoOffsetStore = false;
        configuration.EnableAutoCommit = true;

        var builder = new ConsumerBuilder<TKafkaKey, TKafkaValue>(configuration);
        consumerBuilderOptions?.Invoke(builder);
        using var consumer = builder.Build();
        await ConsumeMultiStepAsync(consumer, settings, read, execute, retire, cancellationToken);
    }

    private static async Task ConsumeMultiStepAsync<TKafkaKey, TKafkaValue, TRead, TExecute>(
        IConsumer<TKafkaKey, TKafkaValue> consumer,
        AtLeastOnceMultiStepSettings settings,
        Func<ConsumeResult<TKafkaKey, TKafkaValue>, CancellationToken, ValueTask<TRead>> read,
        Func<ConsumeResult<TKafkaKey, TKafkaValue>, TRead, CancellationToken, ValueTask<TExecute>> execute,
        Func<ConsumeResult<TKafkaKey, TKafkaValue>, TExecute, CancellationToken, ValueTask> retire,
        CancellationToken cancellationToken)
    {
        using var cancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cancellationToken = cancellationSource.Token;
        var consumeCompletionSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var registerCancelation = cancellationToken.Register(static (state, cancellationToken) => ((TaskCompletionSource)state!).TrySetCanceled(cancellationToken), consumeCompletionSource);
        using var kafkaProcessor = new MultiStepProcessor<TKafkaKey, TKafkaValue, TRead, TExecute>(
            consumer,
            read,
            execute,
            retire,
            settings,
            consumeCompletionSource,
            cancellationToken);

        var consumerTask = new Thread(
            () =>
            {
                try
                {
                    consumer.Subscribe(settings.Topic);
                    try
                    {
                        ConsumeAndMultiStepProcess(consumer, kafkaProcessor, settings.ConsumeTimeout, cancellationToken);
                    }
                    finally
                    {
                        consumer.Close();
                    }
                }
                catch (Exception ex)
                {
                    _ = consumeCompletionSource.TrySetException(ex);
                }
            });

        consumerTask.Start();
        try
        {
            await consumeCompletionSource.Task;
        }
        finally
        {
            await cancellationSource.CancelAsync().ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            await kafkaProcessor.Completion.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            consumerTask.Join();
        }
    }

    private static void ConsumeAndMultiStepProcess<TKafkaKey, TKafkaValue, TRead, TExecute>(
        IConsumer<TKafkaKey, TKafkaValue> consumer,
        MultiStepProcessor<TKafkaKey, TKafkaValue, TRead, TExecute> kafkaProcessor,
        TimeSpan consumeTimeout,
        CancellationToken cancellationToken)
    {
        try
        {
            while (cancellationToken.IsCancellationRequested is false)
            {
                var kafkaMessage = consumer.Consume(consumeTimeout);
                if (kafkaMessage is { } notNull)
                {
                    if (kafkaProcessor.Enqueue(notNull) is false)
                    {
                        return;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _ = kafkaProcessor.Enqueue(ex);
        }
        finally
        {
            _ = kafkaProcessor.Enqueue(Done.Instance);
        }
    }
}