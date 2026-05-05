namespace MultithreatingTests;

using Magj.Multithreating;

public class Tests
{
    [Test]
    public async Task Success()
    {
        static async IAsyncEnumerable<int> GetOne()
        {
            yield return 1;
        }

        Assert.DoesNotThrowAsync(() => ReadExecuteRetire.CreateAsync(
            GetOne(),
            (trigger, token) =>
            {
                Console.WriteLine("read, {0}", trigger);
                return ValueTask.FromResult(trigger.ToString());
            },
            (trigger, fromRead, token) =>
            {
                Console.WriteLine("execute, {0}", fromRead);
                return ValueTask.FromResult(int.Parse(fromRead));
            },
            (trigger, fromExecute, token) =>
            {
                Console.WriteLine("retire, {0}", fromExecute);
                return ValueTask.CompletedTask;
            },
            new()));
    }

    class MyException(string message) : Exception(message);
    [Test]
    public async Task FailtOn3()
    {
        static async IAsyncEnumerable<int> GetOne()
        {
            yield return 1;
            yield return 2;
            yield return 3;
        }

        var task = ReadExecuteRetire.CreateAsync(
            GetOne(),
            (trigger, token) =>
            {
                Console.WriteLine("read, {0}", trigger);
                return ValueTask.FromResult(trigger.ToString());
            },
            (trigger, fromRead, token) =>
            {
                Console.WriteLine("execute, {0}", fromRead);
                return ValueTask.FromResult(int.Parse(fromRead));
            },
            (trigger, fromExecute, token) =>
            {
                if (trigger == 3)
                {
                    throw new MyException("3");
                }
                Console.WriteLine("retire, {0}", fromExecute);
                return ValueTask.CompletedTask;
            },
            new());

        _ = Assert.ThrowsAsync<MyException>(() => task);
    }

    [Test]
    public async Task CancelInfinite()
    {
        static async IAsyncEnumerable<int> GetOne()
        {
            while (true)
            {
                yield return 1;
            }
        }

        using var cancellationTokenSource = new CancellationTokenSource(3);
        var task = ReadExecuteRetire.CreateAsync(
            GetOne(),
            (trigger, token) =>
            {
                Console.WriteLine("read, {0}", trigger);
                return ValueTask.FromResult(trigger.ToString());
            },
            (trigger, fromRead, token) =>
            {
                Console.WriteLine("execute, {0}", fromRead);
                return ValueTask.FromResult(int.Parse(fromRead));
            },
            (trigger, fromExecute, token) =>
            {
                if (trigger == 3)
                {
                    throw new MyException("3");
                }
                Console.WriteLine("retire, {0}", fromExecute);
                return ValueTask.CompletedTask;
            },
            new() { CancellationToken = cancellationTokenSource.Token });

        _ = Assert.ThrowsAsync<TaskCanceledException>(() => task);
    }
}
