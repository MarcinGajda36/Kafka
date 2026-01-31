namespace Magj.Kafka;

using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

// Next step needs to know type of previous step to be type sefe,
// so dedicated 3 step feels better then collection of multistep that require passing object then casting
// altho object + casting can be implemented for traing and it may be cool and usefull 
public sealed record AtLeastOnce3StepsSettings( 
    string Topic,
    string BootstrapServers,
    string GroupId)
{
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
    /// Logger used to log Exceptions.
    /// Fatal kafka exception and Unhandled processor exceptions are still causing throw, so i may not be necessary to assign this.
    /// NullLogger.Instance by default.
    /// </summary>
    public ILogger Logger { get; init; } = NullLogger.Instance;

    /// <summary>
    /// Action to take when there is no initial offset in offset store or the desired offset is out of range.
    /// AutoOffsetReset.Earliest by default.
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; init; } = AutoOffsetReset.Earliest;

}

public sealed partial class KafkaClient
{

}
