using BenchmarkDotNet.Attributes;
using Proto.Utilities.Benchmark;
using QueryPlannerTest;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using Buffer = SimpleDB.Data.Buffer;

namespace SimpleDbNET.Benchmarks
{
    [MemoryDiagnoser(false)]
    public class BufferManagerBenchmarks
    {
        private BenchmarkThreadHelper threadHelper;
        private BufferManager bufferManager;
        private readonly string fileName = "testfile";

        BlockId blockId;

        private readonly Action pinBlockAction;

        public BufferManagerBenchmarks()
        {
            pinBlockAction = () =>
            {
                Buffer buffer = bufferManager.PinBlock(blockId);
                bufferManager.UnpinBuffer(buffer);
            };

            blockId = BlockId.New(fileName, 1);
        }


        [GlobalSetup(Target = nameof(PinBlock))]
        public void SetupPinBlock()
        {
            var fileManager = new FileManager("bufferManagerBenchmarks", 4096, new EmptyBlocksReadWriteTracker(), true);
            var logManager = new LogManager(fileManager, "log");

            bufferManager = new BufferManager(fileManager, logManager, 8);

            threadHelper = new BenchmarkThreadHelper();
            threadHelper.AddAction(pinBlockAction);
            threadHelper.AddAction(pinBlockAction);
            threadHelper.AddAction(pinBlockAction);
            threadHelper.AddAction(pinBlockAction);
            threadHelper.AddAction(pinBlockAction);
            threadHelper.AddAction(pinBlockAction);
            threadHelper.AddAction(pinBlockAction);
            threadHelper.AddAction(pinBlockAction);
            threadHelper.AddAction(pinBlockAction);
            threadHelper.AddAction(pinBlockAction);
        }

        [Benchmark]
        public void PinBlock()
        {
            threadHelper.ExecuteAndWait();
        }
    }
}
