using QueryPlannerTest;
using SimpleDb.Transactions.Concurrency;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Metadata;
using SimpleDB.Plan;
using SimpleDB.Query;
using SimpleDB.Tx;

public class Program
{
    public static void Main(string[] args)
    {
        var fileManager = new FileManager("PlannerTest", 4096, new EmptyBlocksReadWriteTracker(), false);
        var logManager = new LogManager(fileManager, "log");
        var bufferManager = new BufferManager(fileManager, logManager, 3);
        var lockTable = new LockTable();
        Random random = new Random();

        Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable);

        Transaction tx = newTx();
        MetadataMgr mdm = new MetadataMgr(true, tx);

        QueryPlanner qp = new BasicQueryPlanner(mdm);
        UpdatePlanner up = new BasicUpdatePlanner(mdm);
        Planner planner = new Planner(qp, up);

        /*String cmd = "create table T1(A int not null, B varchar(9))";
        planner.executeUpdate(cmd, tx);

        int n = 200;
        Console.WriteLine("Inserting " + n + " random records.");
        for (int i = 0; i < n; i++)
        {
            int a = random.Next(1, 50);
            String b = "rec" + a;
            cmd = "insert into T1(A,B) values(" + a + ", '" + b + "')";
            planner.executeUpdate(cmd, tx);
        }*/

        for(int i = 0; i < 1000; i++)
        {
            Plan p = planner.createQueryPlan($"select A,B from T1 where A<{i}", tx);
        }

        /*String qry = "select A,B from T1 where A<10";
        Plan p = planner.createQueryPlan(qry, tx);
        Scan s = p.open();
        while (s.next())
        {
            Console.WriteLine($"A = {s.getInt("A")}");
        }
        s.close();*/
        tx.Commit();
    }
}