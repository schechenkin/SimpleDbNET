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
    public class TableManagerTest
    {
        [Fact]
        public void Test()
        {
            var fileManager = new FileManager("TableManagerTest", 400, new TestBlocksReadWriteTracker(), TestLoggerFactory.Instance, true);
            var logManager = new LogManager(fileManager, "log", TestLoggerFactory.Instance);
            var bufferManager = new BufferManager(fileManager, logManager, 3, TestLoggerFactory.Instance);
            var lockTable = new LockTable();

            Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable, TestLoggerFactory.Instance);
            
            Transaction tx = newTx();

            TableMgr tableManager = new TableMgr(true, tx);

            Schema schema = new Schema();
            schema.AddIntColumn("A");
            schema.AddStringColumn("B", 9, true);
            tableManager.createTable("MyTable", schema, tx);

            schema = new Schema();
            schema.AddIntColumn("D");
            schema.AddStringColumn("E", 9, true);
            tableManager.createTable("MyTable2", schema, tx);

            tx.Commit();
            tx = newTx();

            fileManager.ReopenFiles();

            Layout layout = tableManager.getLayout("MyTable", tx);
            layout.slotSize().Should().BeGreaterThan(0);

            Schema sch2 = layout.schema();
            sch2.ColumnNames().Should().BeEquivalentTo("A", "B");

            sch2.GetSqlType("A").Should().Be(SqlType.INTEGER);
            sch2.GetColumnLength("A").Should().Be(4);
            sch2.IsNullable("A").Should().BeFalse();
            sch2.GetSqlType("B").Should().Be(SqlType.VARCHAR);
            sch2.GetColumnLength("B").Should().Be(9);
            //sch2.IsNullable("B").Should().BeTrue();

            tx.Commit();
        }
    }
}
