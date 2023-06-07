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

public class IndexUpdateTest
{
    [Fact]
    public void Test()
    {
        var fileManager = new FileManager("IndexUpdateTest", 1024, true);
        var logManager = new LogManager(fileManager, "log");
        var bufferManager = new BufferManager(fileManager, logManager, 1000);
        var lockTable = new LockTable();

        Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable);

        Transaction tx = newTx();
        MetadataMgr mdm = new MetadataMgr(true, tx);

        Schema sch = new Schema();
        sch.AddIntColumn("Id");
        sch.AddStringColumn("Name", 16);
        Layout layout = new Layout(sch);

        mdm.createTable("student", sch, tx);

        Plan studentPlan = new TablePlan(tx, "student", mdm);
        UpdateScan studentscan = (UpdateScan)studentPlan.open();

        for (int i = 1; i <= 100; i++)
        {
            studentscan.Insert();
            studentscan.SetValue("Id", i);
            studentscan.SetValue("Name", new DbString($"Name_{i}"));
        }

        mdm.createIndex("studentId", "student", "Id", tx);
        mdm.createIndex("studentName", "student", "Name", tx);

        // Create a map containing all indexes for STUDENT.
        Dictionary<string, SimpleDb.Index> indexes = new Dictionary<string, SimpleDb.Index>();
        Dictionary<string, IndexInfo> idxinfo = mdm.getIndexInfo("student", tx);
        foreach (string fldname in idxinfo.Keys)
        {
            var index = idxinfo[fldname].open();
            indexes.Add(fldname, index);
        }

        //Fill indexes
        TableScan tableScan = new TableScan(tx, "student", layout);
        tableScan.BeforeFirst();
        while (tableScan.Next())
        {
            //var rid = tableScan.GetRid();
            //var id = tableScan.GetInt("Id");
            
            indexes["Id"].insert(tableScan.GetInt("Id"), tableScan.GetRid());
            indexes["Name"].insert(tableScan.GetString("Name"), tableScan.GetRid());
        }

        //find name with Id = 49
        var idIndex = indexes["Id"];
        idIndex.beforeFirst(49);
        idIndex.next().Should().BeTrue();
        var idRid = idIndex.getDataRid();

        tableScan.MoveToRid(idRid);
        string name = tableScan.GetString("Name").GetString();
        name.Should().Be($"Name_{49}");

        /*var nameIndex = indexes["Name"];
        nameIndex.beforeFirst((DbString)$"Name_{49}");
        nameIndex.next().Should().BeTrue();
        var nameRid = nameIndex.getDataRid();
        tableScan.MoveToRid(nameRid);
        tableScan.GetInt("Id").Should().Be(49);*/

        tableScan.Close();
        tx.Commit();
    }
}