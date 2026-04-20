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

        await ReadExecuteRetire.CreateAsync(
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
            new());

        Assert.Pass();
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

        await Task.Delay(100);
        Assert.ThrowsAsync<MyException>(() => task);
    }
}
