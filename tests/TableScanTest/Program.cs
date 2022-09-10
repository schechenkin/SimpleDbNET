using SimpleDb.Transactions.Concurrency;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Record;
using SimpleDB.Tx;
using System.Diagnostics;

public class Program
{
    public static void Main(string[] args)
    {
        var fileManager = new FileManager("TableScanTest", 4096, false);
        var logManager = new LogManager(fileManager, "log");
        var bufferManager = new BufferManager(fileManager, logManager, 1000);
        var lockTable = new LockTable();
        Random random = new Random();

        Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable);

        Transaction tx = newTx();

        string A_column = "A";
        string B_column = "B";
        //string text = "rec5000";

        Schema sch = new Schema();
        sch.AddIntColumn(A_column);
        sch.AddStringColumn(B_column, 9);
        Layout layout = new Layout(sch);

        //Filling the table
        TableScan tableScan = new TableScan(tx, "T", layout);
        /*for (int i = 0; i < 400_000; i++)
        {
            tableScan.insert();
            int n = random.Next(0, 400_000);
            tableScan.setInt(A_column, n);
            tableScan.setString(B_column, "rec" + n);

            if (i % 5000 == 0)
                Console.WriteLine($"{i} records inserted");
        }
        tx.Commit();
        Console.WriteLine("all records inserted");*/

        //Deleting these records, whose A-values are less than 25
        /*int deleteCount = 0;
        tableScan.beforeFirst();
        while (tableScan.next())
        {
            int a = tableScan.getInt("A");
            if (a < 25)
            {
                deleteCount++;
                tableScan.delete();
            }
        }*/

        Console.WriteLine("start scan");
        int remaining = 0;
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();
        tableScan.beforeFirst();
        while (tableScan.next())
        {
            int a = tableScan.getInt(A_column);
            if (a < 0)
                throw new Exception();
            //Console.WriteLine(a);
            string b = tableScan.getString(B_column);
            if (string.IsNullOrEmpty(b))
                throw new Exception();
            var rid = tableScan.getRid();
            if (rid.blockNumber() < 0)
                throw new Exception();

            //if (remaining > 100_000)
            //    break;

            remaining++;
        }
        tableScan.close();
        stopwatch.Stop();
        tx.Commit();

        Console.WriteLine($"scan complete {remaining} records");
        Console.WriteLine($"time ms {stopwatch.ElapsedMilliseconds}");
    }
}