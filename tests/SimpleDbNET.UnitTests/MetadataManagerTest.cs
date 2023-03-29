using FluentAssertions;
using SimpleDb.Transactions.Concurrency;
using SimpleDB;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Metadata;
using SimpleDB.Record;
using SimpleDB.Tx;
using SimpleDbNET.UnitTests.Fixtures;
using Xunit;

namespace SimpleDbNET.UnitTests
{
    public class MetadataManagerTest
    {
        [Fact]
        public void Test()
        {
            var fileManager = new FileManager("MetadataManagerTest", 400, new TestBlocksReadWriteTracker(), TestLoggerFactory.Instance, true);
            var logManager = new LogManager(fileManager, "log", TestLoggerFactory.Instance);
            var bufferManager = new BufferManager(fileManager, logManager, 3, TestLoggerFactory.Instance);
            var lockTable = new LockTable();
            Random random = new Random();

            Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable, TestLoggerFactory.Instance);
            
            Transaction tx = newTx();
            MetadataMgr mdm = new MetadataMgr(true, tx);

            Schema sch = new Schema();
            sch.AddIntColumn("A");
            sch.AddStringColumn("B", 9);

            // Part 1: Table Metadata
            mdm.createTable("MyTable", sch, tx);
            Layout layout = mdm.getLayout("MyTable", tx);
            int size = layout.slotSize();
            Schema sch2 = layout.schema();

            sch2.ColumnNames().Should().BeEquivalentTo("A", "B");
            sch2.GetSqlType("A").Should().Be(SqlType.INTEGER);
            sch2.GetColumnLength("A").Should().Be(4);
            sch2.GetSqlType("B").Should().Be(SqlType.VARCHAR);
            sch2.GetColumnLength("B").Should().Be(9);

            // Part 2: Statistics Metadata
            TableScan ts = new TableScan(tx, "MyTable", layout);
            for (int i = 0; i < 50; i++)
            {
                ts.insert();
                int n = random.Next(0, 49);
                ts.setInt("A", n);
                ts.setString("B", "rec" + n);
            }
            StatInfo si = mdm.getStatInfo("MyTable", layout, tx);
            si.blocksAccessed().Should().BeGreaterThan(0);
            si.recordsOutput().Should().BeGreaterThan(0);
            si.distinctValues("A").Should().BeGreaterThan(0);
            si.distinctValues("B").Should().BeGreaterThan(0);

            // Part 3: View Metadata     
            //string viewdef = "select B from MyTable where A = 1";
            //mdm.createView("viewA", viewdef, tx);
            //mdm.getViewDef("viewA", tx).Should().Be(viewdef);

            // Part 4: Index Metadata
            /*mdm.createIndex("indexA", "MyTable", "A", tx);
            mdm.createIndex("indexB", "MyTable", "B", tx);
            var idxmap = mdm.getIndexInfo("MyTable", tx);

            IndexInfo ii = idxmap["A"];
            ii.blocksAccessed().Should().BeGreaterThan(0);
            ii.recordsOutput().Should().BeGreaterThan(0);
            ii.distinctValues("A").Should().BeGreaterThan(0);
            ii.distinctValues("B").Should().BeGreaterThan(0);

            ii = idxmap["B"];
            ii.blocksAccessed().Should().BeGreaterThan(0);
            ii.recordsOutput().Should().BeGreaterThan(0);
            ii.distinctValues("A").Should().BeGreaterThan(0);
            ii.distinctValues("B").Should().BeGreaterThan(0);*/

            tx.Commit();
        }
    }
}
