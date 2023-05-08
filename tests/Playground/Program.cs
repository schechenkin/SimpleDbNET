using CommandLine;
using Playground.LockTableTest;

public class Program
{
    public static void Main(string[] args)
    {
        CommandLine.Parser.Default.ParseArguments<Options>(args)
            .WithParsed(Run)
            .WithNotParsed(HandleParseError);
    }

    static void Run(Options opts)
    {
        TestRunner test = new TestRunner(new LockTableWithReadWriteLock(), opts);
        //TestRunner test = new TestRunner(new LockTableSimple2(), opts);

        test.Run();
    }
    static void HandleParseError(IEnumerable<Error> errs)
    {
        foreach (var err in errs)
        {
            Console.WriteLine(err.ToString());
        }
    }
}