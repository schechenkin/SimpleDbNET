using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using QueryPlannerTest;
using SimpleDB.file;

namespace SimpleDbNET.Benchmarks
{
    [SimpleJob(RuntimeMoniker.Net60)]
    [MemoryDiagnoser]
    public class FileManagerBenchmark
    {
        FileManager fileManager;
        string importantString;
        int stringSize;
        Page page;
        private Random r = new Random();
        BlockId blockId;

        [GlobalSetup]
        public void ConfigureBufferManager()
        {
            fileManager = new FileManager("FileManagerBenchmark", 4096, new EmptyBlocksReadWriteTracker());
            importantString = "important string";
            stringSize = Page.CalculateStringStoringSize(importantString);
            page = new Page(fileManager.BlockSize);
            blockId = BlockId.New("testfile", 0);
        }

        [Benchmark]
        public Page ReadBlock()
        {
            blockId.SetNumber(r.Next(0, 10000));
            fileManager.ReadBlock(blockId, page);
            return page;
        }

        [Benchmark]
        public Page WritePage()
        {
            blockId.SetNumber(r.Next(0, 10000));
            fileManager.WritePage(page, blockId);
            return page;
        }
    }
}
