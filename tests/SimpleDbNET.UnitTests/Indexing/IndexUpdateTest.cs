using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Record;
using SimpleDb.Transactions.Concurrency;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using SimpleDB.Tx;
using SimpleDB.Metadata;
using SimpleDB.Plan;
using SimpleDB.Query;
using FluentAssertions;
using SimpleDbNET.UnitTests.Fixtures;

namespace SimpleDbNET.UnitTests.Indexing
{
    public class IndexUpdateTest
    {
        [Fact]
        public void Test()
        {
            var fileManager = new FileManager("IndexUpdateTest", 1024, new TestBlocksReadWriteTracker(), TestLoggerFactory.Instance, true);
            var logManager = new LogManager(fileManager, "log", TestLoggerFactory.Instance);
            var bufferManager = new BufferManager(fileManager, logManager, 1000, TestLoggerFactory.Instance);
            var lockTable = new LockTable();

            Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable, TestLoggerFactory.Instance);

            Transaction tx = newTx();
            MetadataMgr mdm = new MetadataMgr(true, tx);

            Schema sch = new Schema();
            sch.AddIntColumn("Id");
            sch.AddStringColumn("Name", 16);
            Layout layout = new Layout(sch);

            mdm.createTable("student", sch, tx);

            Plan studentPlan = new TablePlan(tx, "student", mdm);
            UpdateScan studentscan = (UpdateScan)studentPlan.open();

            for(int i = 1; i <= 100; i++) 
            {
                studentscan.insert();
                studentscan.setInt("Id", i);
                studentscan.setString("Name", $"Name_{i}");
            }

            mdm.createIndex("studentId", "student", "Id", tx);
            mdm.createIndex("studentName", "student", "Name", tx);

            // Create a map containing all indexes for STUDENT.
            Dictionary<string, SimpleDB.Indexes.Index> indexes = new Dictionary<string, SimpleDB.Indexes.Index>();
            Dictionary<string, IndexInfo> idxinfo = mdm.getIndexInfo("student", tx);
            foreach (string fldname in idxinfo.Keys)
            {
                var index = idxinfo[fldname].open();
                indexes.Add(fldname, index);
            }

            //Fill indexes
            TableScan tableScan = new TableScan(tx, "student", layout);
            tableScan.beforeFirst();
            while (tableScan.next())
            {
                indexes["Id"].insert(new Constant(tableScan.getInt("Id")), tableScan.getRid());
                indexes["Name"].insert(new Constant(tableScan.getString("Name")), tableScan.getRid());
            }

            //find name with Id = 49
            var idIndex = indexes["Id"];
            idIndex.beforeFirst(new Constant(49));
            idIndex.next().Should().BeTrue();
            var idRid = idIndex.getDataRid();

            tableScan.moveToRid(idRid);
            string name = tableScan.getString("Name");
            name.Should().Be($"Name_{49}");

            tableScan.close();
            tx.Commit();
        }
    }
}
