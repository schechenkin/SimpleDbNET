using FluentAssertions;
using SimpleDb.Buffers;
using SimpleDb.File;
using SimpleDb.Log;
using SimpleDb.Record;
using SimpleDb.Transactions;
using SimpleDb.Transactions.Concurrency;
using SimpleDb.Types;
using SimpleDB.Metadata;
using Xunit;

namespace SimpleDbNET.UnitTests.Tests;

public class TableScanTest
{
    [Fact]
    public void Insert_and_delete()
    {
        var fileManager = new FileManager("TableScanTest", 400, true);
        var logManager = new LogManager(fileManager, "log");
        var bufferManager = new BufferManager(fileManager, logManager, 3);
        var lockTable = new LockTable();
        Random random = new Random();

        Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable);

        Transaction tx = newTx();

        Schema sch = new Schema();
        sch.AddIntColumn("A");
        sch.AddStringColumn("B", 9);
        sch.AddStringColumn("C", 5, nullable: true);
        sch.AddDateTimeColumn("D");
        Layout layout = new Layout(sch);

        //Filling the table with 50 random records
        DateTime dt = new DateTime(2022, 09, 30);
        TableScan tableScan = new TableScan(tx, "T", layout);
        for (int i = 0; i < 50; i++)
        {
            tableScan.Insert();
            int n = random.Next(0, 49);
            tableScan.SetValue("A", n);
            tableScan.SetValue("B", (DbString)("rec" + n));
            if (n % 2 == 0)
            {
                tableScan.setNull("C");
            }
            else
            {
                tableScan.SetValue("C", (DbString)("rec" + n));
            }
            tableScan.SetValue("D", dt);
        }

        //Deleting these records, whose A-values are less than 25
        int deleteCount = 0;
        tableScan.BeforeFirst();
        while (tableScan.Next())
        {
            int a = tableScan.GetInt("A");
            if (a < 25)
            {
                deleteCount++;
                tableScan.Delete();
            }
        }
        deleteCount.Should().BeGreaterThan(0);

        tableScan.Close();
        tx.Commit();

        //check remaining records
        tableScan = new TableScan(newTx(), "T", layout);
        int remaining = 0;
        tableScan.BeforeFirst();
        while (tableScan.Next())
        {
            int A = tableScan.GetInt("A");

            A.Should().BeGreaterThan(0);
            tableScan.GetString("B").GetString().Should().NotBeNullOrEmpty();
            tableScan.GetDateTime("D").Should().Be(dt);
            if (A % 2 == 0)
            {
                tableScan.IsNull("C").Should().BeTrue();
            }
            else
            {
                tableScan.IsNull("C").Should().BeFalse();
                tableScan.GetString("C").Should().Be($"rec" + A);
                //tableScan.CompareString("C", new StringConstant($"rec" + A)).Should().BeTrue();
                //tableScan.CompareString("C", new StringConstant($"random text")).Should().BeFalse();
            }
            remaining++;
        }
        tableScan.Close();
        tx.Commit();

        remaining.Should().BeGreaterThan(0);
    }
}