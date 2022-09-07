using BenchmarkDotNet.Running;
using SimpleDbNET.Benchmarks;

namespace MyBenchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<StringTokenizerBenchmark>();
        }
    }
}