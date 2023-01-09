using SimpleDb.file;
using SimpleDb.Transactions.Concurrency;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Query;
using SimpleDB.Record;
using SimpleDB.Tx;
using System.Diagnostics;

public class Program
{
    static string A_column = "A";
    static string B_column = "B";

    public static void Main(string[] args)
    {
        var fileManager = new FileManager("TableScanTest", 4096, new EmptyBlocksReadWriteTracker(), false);
        var logManager = new LogManager(fileManager, "log");
        var bufferManager = new BufferManager(fileManager, logManager, 500);
        var lockTable = new LockTable();
        Random random = new Random();

        Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable);

        Transaction tx = newTx();

        Schema sch = new Schema();
        sch.AddIntColumn(A_column);
        sch.AddStringColumn(B_column, 9);
        Layout layout = new Layout(sch);

        //Filling the table
        TableScan tableScan = new TableScan(tx, "T", layout);
        //CreateRecords(tx, A_column, B_column, tableScan);

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

        DoMeasurement(tableScan);
        //DoMeasurement(tableScan);
        //DoMeasurement(tableScan);
        tableScan.close();
        tx.Commit();

    }

    private static void DoMeasurement(TableScan tableScan)
    {
        tableScan.beforeFirst();
        Console.WriteLine("start scan");
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();
        DoSelectScan(tableScan);
        stopwatch.Stop();
        Console.WriteLine($"time ms {stopwatch.ElapsedMilliseconds}");
    }

    private static void CreateRecords(Transaction tx, TableScan tableScan)
    {
        for (int i = 0; i < 400_000; i++)
        {
            tableScan.insert();
            //int n = random.Next(0, 400_000);
            int n = i;
            tableScan.setInt(A_column, n);
            tableScan.setString(B_column, "rec" + n);

            if (i % 5000 == 0)
                Console.WriteLine($"{i} records inserted");
        }
        tx.Commit();
        Console.WriteLine("all records inserted");
    }

    private static void DoSelectScan(TableScan tableScan)
    {
        Term term = new Term(new Expression(A_column), new Expression(new Constant(79871)));
        Predicate pred = new Predicate(term);
        Scan selectScan = new SelectScan(tableScan, pred);

        while (selectScan.next())
        {
            int a = selectScan.getInt(A_column);
            string b = selectScan.getString(B_column);

            Console.WriteLine($"A = {a}, b = {b}");
        }
    }

    private static void DoTableScan(TableScan tableScan)
    {
        tableScan.beforeFirst();
        while (tableScan.next())
        {
            int a = tableScan.getInt(A_column);
            if (a == 79871)
            {
                string b = tableScan.getString(B_column);
                Console.WriteLine($"A = {a}, b = {b}");
            }
            if (a < 0)
                throw new Exception();
        }
    }

    internal class EmptyBlocksReadWriteTracker : IBlocksReadWriteTracker
    {
        public int BlocksRead { get; set; }

        public int BlocksWrite { get; set; }

        public void TrackBlockRead()
        {

        }

        public void TrackBlockWrite()
        {

        }
    }
}