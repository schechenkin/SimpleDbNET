using System.Diagnostics;
using SimpleDb.File;
using SimpleDb.Transactions.Concurrency;

namespace Playground.LockTableTest;

public class TestRunner
{
    Options options;
    ILockTable lockTable;

    Random rnd = new Random();

    int buffersCount = 100;
    Dictionary<int, int> writeCounts = new Dictionary<int, int>();

    public TestRunner(ILockTable lockTable, Options options)
    {
        this.options = options;
        this.lockTable = lockTable;
        for (int i = 0; i <= buffersCount; i++)
        {
            writeCounts.Add(i, 0);
        }
    }

    /*static void Produce(LockTable lockTable, Options options)
    {
        Random rnd = new Random();
        
        while(true)
        {
            if(Interlocked.Read(ref SharedRequests) > options.SharedLocks)
                break;

            int num = rnd.Next(1, 100);
            var blockId = BlockId.New("filename", num);
            
            lockTable.WaitSharedLock(blockId);
            if(options.ProduceTimeout > 0)
                Thread.Sleep(options.ProduceTimeout);
            lockTable.UnLock(blockId);

            Interlocked.Increment(ref SharedRequests);
        }
    }*/

    private void Reader()
    {
        for (int i = 0; i < options.SharedLocks; i++)
        {
            BlockId blockId = BlockId.New("testfile", rnd.Next(0, buffersCount));
            lockTable.WaitSharedLock(blockId);
            if (options.ReadersTimeout > 0)
                Thread.Sleep(options.ReadersTimeout);
            lockTable.UnLock(blockId);
        }
    }

    private void Writer()
    {
        for (int i = 0; i < options.ExclusiveLocks; i++)
        {
            BlockId blockId = BlockId.New("testfile", rnd.Next(0, buffersCount));
            lockTable.WaitExclusiveLock(blockId);
            if (options.WritersTimeout > 0)
                Thread.Sleep(options.WritersTimeout);
            //Interlocked.Increment(ref writeCount);
            //writeCount++;
            writeCounts[blockId.Number]++;
            lockTable.UnLock(blockId);
        }
    }

    public void Run()
    {
        List<Task> writers = new List<Task>();
        for (int i = 0; i < options.Writers; i++)
        {
            writers.Add(new Task(Writer));
        }

        List<Task> readers = new List<Task>();
        for (int i = 0; i < options.Readers; i++)
        {
            readers.Add(new Task(Reader));
        }

        Stopwatch stopWatch = new Stopwatch();
        stopWatch.Start();

        for (int i = 0; i < writers.Count; i++)
        {
            writers[i].Start();
        }

        for (int i = 0; i < readers.Count; i++)
        {
            readers[i].Start();
        }

        Task.WhenAll(writers).Wait();

        stopWatch.Stop();

        int totalWrites = 0;
        foreach(var kvp in writeCounts)
        {
            totalWrites += kvp.Value;
        }

        Console.WriteLine($"RunTime {stopWatch.ElapsedMilliseconds} writeCount {totalWrites}");




        /*
        
        List<Thread> Producers = new List<Thread>();

        for (int i = 0; i < options.NProd; i++)
        {
            Producers.Add(new Thread(() => Produce(lockTable, options)));
        }

        Stopwatch stopWatch = new Stopwatch();
        stopWatch.Start();

        foreach (var t in Producers)
            t.Start();

        foreach (var t in Producers)
            t.Join();


        stopWatch.Stop();
        Console.WriteLine("RunTime " + stopWatch.ElapsedMilliseconds);
        //Console.WriteLine("Processed " + NTasksProcessed);
        */
    }
}
