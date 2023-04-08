using FluentAssertions;
using SimpleDb.Buffers;
using SimpleDb.File;
using SimpleDb.Log;
using SimpleDb.Metadata;
using SimpleDb.Plan;
using SimpleDb.Query;
using SimpleDb.Record;
using SimpleDb.Transactions;
using SimpleDb.Transactions.Concurrency;
using SimpleDb.Types;
using SimpleDB.Metadata;
using Xunit;

namespace SimpleDbNET.UnitTests.Tests;

public class PlannerTest
{
    [Fact]
    public void Create_Insert_Select()
    {
        var fileManager = new FileManager("PlannerTest", 400, true);
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

        String cmd = "create table T1(A int not null, B varchar(9))";
        planner.executeUpdate(cmd, tx);

        int n = 200;
        Console.WriteLine("Inserting " + n + " random records.");
        for (int i = 1; i <= n; i++)
        {
            String b = "rec" + i;
            cmd = "insert into T1(A,B) values(" + i + ", '" + b + "')";
            planner.executeUpdate(cmd, tx);
        }

        String qry = "select B from T1 where A=10";
        Plan p = planner.createQueryPlan(qry, tx);
        Scan s = p.open();
        while (s.Next())
        {
            s.GetString("B").Should().Be("rec10");
        }
        s.Close();

        qry = "select A from T1 where B='rec10'";
        p = planner.createQueryPlan(qry, tx);
        s = p.open();
        while (s.Next())
        {
            s.GetInt("A").Should().Be(10);
        }
        s.Close();

        tx.Commit();
    }
}