
using BenchmarkDotNet.Running;
using Playground;

public class Program
{
    public static void Main(string[] args)
    {
        var summary = BenchmarkRunner.Run<StructsBenchmark>();
    }
}