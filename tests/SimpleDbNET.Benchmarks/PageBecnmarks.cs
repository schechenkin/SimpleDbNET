using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using SimpleDB.file;

namespace SimpleDbNET.Benchmarks
{
    [SimpleJob(RunStrategy.Throughput, RuntimeMoniker.Net60)]
    [MemoryDiagnoser]
    [HtmlExporter]
    public class PageBecnmarks
    {
        Page page;
        FileManager fileManager;
        Random r = new Random();
        string importantString;
        byte[] buffer;

        [GlobalSetup]
        public void ConfigureBufferManager()
        {
            fileManager = new FileManager("PageBecnmarks", 4096);
            importantString = "important string";
            page = new Page(fileManager.BlockSize);
            buffer = new byte[100];
            r.NextBytes(buffer);
        }

        [Benchmark]
        public Page CreatePage()
        {
            return new Page(fileManager.BlockSize);
        }

        [Benchmark]
        public Page SetString()
        {
            page.SetString(r.Next(0, 4000), importantString);
            return page;
        }

        [Benchmark]
        public Page SetInt()
        {
            page.SetInt(r.Next(0, 4000), 42);
            return page;
        }

        [Benchmark]
        public Page SetBytes()
        {
            page.SetBytes(r.Next(0, 3000), buffer);
            return page;
        }
    }
}
