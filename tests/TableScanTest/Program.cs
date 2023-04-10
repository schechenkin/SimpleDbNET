using SimpleDb.File;
using SimpleDb.Query;
using SimpleDb.Transactions.Concurrency;
using SimpleDb.Log;
using SimpleDb.Record;
using SimpleDb.Transactions;
using System.Diagnostics;
using SimpleDb.Buffers;
using SimpleDb.Types;
using SimpleDB.Query;

public readonly struct RRS
{
    public RRS(int id)
    {
        Id = id;
    }

    public int Id { get; }
}

public class Program
{
    static string A_column = "A";
    static string B_column = "B";
    static Schema sch;
    static Layout layout;

    public static void Main(string[] args)
    {
        //Dictionary<RRS, string> dict = new Dictionary<RRS, string>();
        
        var fileManager = new FileManager("TableScanTest", 4096, false, 4096);
        var fileManagerForLogmanager = new FileManager("TableScanTest", 1024*1024*16, false, 100);
        var logManager = new LogManager(fileManagerForLogmanager, "log");
        var bufferManager = new BufferManager(fileManager, logManager, 50000);
        var lockTable = new LockTable();
        Random random = new Random();

        Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable);

        Transaction tx = newTx();

        sch = new Schema();
        sch.AddIntColumn(A_column);
        sch.AddStringColumn(B_column, 9);
        layout = new Layout(sch);

        //Filling the table
        TableScan tableScan = new TableScan(tx, "T", layout);
        //CreateRecords(tx, tableScan, 1_000_000);
        //bufferManager.FlushAll(tx.Number);

        bufferManager.Print(false);
        DoMeasurement(tableScan);
        Thread.Sleep(1000);
        bufferManager.Print(false);
        DoMeasurement(tableScan);
        Thread.Sleep(1000);
        bufferManager.Print(false);
        DoMeasurement(tableScan);
        bufferManager.Print(false);

        tableScan.Close();
        tx.Commit();

        //bufferManager.Print();

    }

    private static void DoMeasurement(TableScan tableScan)
    {
        tableScan.BeforeFirst();
        Console.WriteLine("start scan");
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();
        //DoSelectScan(tableScan);
        DoTableScan(tableScan);
        stopwatch.Stop();
        Console.WriteLine($"time ms {stopwatch.ElapsedMilliseconds}");
    }

    private static void CreateRecords(Transaction tx, TableScan tableScan, int count)
    {
        for (int i = 0; i < count; i++)
        {
            tableScan.Insert();
            //int n = random.Next(0, 400_000);
            int n = i;
            tableScan.SetValue(A_column, n);
            tableScan.SetValue<DbString>(B_column, "rec" + n);

            if (i % 5000 == 0)
                Console.WriteLine($"{i} records inserted");
        }
        tx.Commit();
        Console.WriteLine("all records inserted");
    }

    private static void DoSelectScan(TableScan tableScan)
    {
        Term term = new Term(new Expression(B_column), new Expression((DbString)"rec79871"));
        Predicate pred = new Predicate(term);
        Scan selectScan = new SelectScan(tableScan, pred);

        while (selectScan.Next())
        {
            int a = selectScan.GetInt(A_column);
            var b = selectScan.GetString(B_column);

            Console.WriteLine($"A = {a}, b = {b}");
        }
    }

    private static void DoTableScan(TableScan tableScan)
    {
        int counter = 0;
        tableScan.BeforeFirst();
        while (tableScan.Next())
        {
            /*if (layout.schema().GetSqlType(A_column) == SqlType.INTEGER)
            {
                var cnst = new ConstantRefStruct(tableScan.getInt(A_column));
                if (cnst.asInt() == 79871)
                {
                    string b = tableScan.getString(B_column);
                    Console.WriteLine($"A = {cnst.asInt()}, b = {b}");
                }
            }*/

            counter++;

            int a = tableScan.GetInt(A_column);
            if (a == 79871)
            {
                var b = tableScan.GetString(B_column);
                Console.WriteLine($"A = {a}, b = {b}");
            }
            if (a < 0)
                throw new Exception();
        }

        Console.WriteLine(counter);
    }
}