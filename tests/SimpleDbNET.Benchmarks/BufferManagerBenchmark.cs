using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using Buffer = SimpleDB.Data.Buffer;

namespace SimpleDbNET.Benchmarks
{
    [SimpleJob(RunStrategy.Monitoring, RuntimeMoniker.Net60)]
    [SimpleJob(launchCount: 1, warmupCount: 1, targetCount: 1)]
    [MemoryDiagnoser]
    [ThreadingDiagnoser]
    public class BufferManagerBenchmark
    {
        private BufferManager bufferManager;
        private readonly string fileName = "testfile";
        private Random r = new Random();
        BlockId blockId;

        [GlobalSetup]
        public void ConfigureBufferManager()
        {
            var fileManager = new FileManager("bufferManagerBenchmark", 4096, true);
            var logManager = new LogManager(fileManager, "log");

            bufferManager = new BufferManager(fileManager, logManager, 8);
        }

        [IterationSetup]
        public void IterationSetup()
        {
            blockId = BlockId.New(fileName, r.Next(0, 100));
        }

        [Benchmark]
        public Buffer PinUnpinBlock()
        {
            Buffer buffer = bufferManager.PinBlock(blockId);
            bufferManager.UnpinBuffer(buffer);
            return buffer;
        }
    }
}
