using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Record;
using SimpleDb.Transactions.Concurrency;
using SimpleDB.Tx;
using System.Diagnostics;

namespace Playground
{
    internal class TableScanInsert
    {
        public void Run()
        {
            var fileManager = new FileManager("Playground", 4096, new EmptyBlocksReadWriteTracker(), true);
            var logManager = new LogManager(fileManager, "log");
            var bufferManager = new BufferManager(fileManager, logManager, 100500);
            var lockTable = new LockTable();

            Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable);

            Schema sch = new Schema();
            sch.AddIntColumn("Id");
            sch.AddStringColumn("Name", 10);

            Layout layout = new Layout(sch);

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            Transaction tx = newTx();
            TableScan tableScan = new TableScan(tx, "Test", layout);

            for (int i = 1; i <= 3000; i++)
            {
                tableScan.insert();
                tableScan.setInt("Id", i);
                tableScan.setString("Name", "rec" + i);

                if(i % 100 == 0)
                    Console.WriteLine($"{i} inserted");
            }


            tableScan.close();
            tx.Commit();

            stopwatch.Stop();
            Console.WriteLine($"time ms {stopwatch.ElapsedMilliseconds}");
        }
    }
}
