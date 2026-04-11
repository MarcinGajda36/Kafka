namespace MultithreatingTests;

using Magj.Multithreating;

public class Tests
{
    [Test]
    public async Task Test1()
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
}
