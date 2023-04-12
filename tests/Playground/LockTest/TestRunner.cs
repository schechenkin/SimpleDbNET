using System.Diagnostics;
using SimpleDb.File;
using SimpleDb.Transactions.Concurrency;

namespace Playground.LockTest;

public class TestRunner
{
    static long SharedRequests;
    static long ExclusiveRequests;

    Options options;
    LockTable lockTable;
    
    public TestRunner(Options options)
    {
        SharedRequests = 0;
        ExclusiveRequests = 0;
        this.options = options;
        this.lockTable = new LockTable();
    }

    static void Produce(LockTable lockTable, Options options)
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
    }

    public void Run()
    {
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
    }
}
