using FluentAssertions;
using SimpleDb.Transactions.Concurrency;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Record;
using SimpleDB.Tx;
using SimpleDbNET.UnitTests.Fixtures;
using Xunit;

namespace SimpleDbNET.UnitTests
{
    public class ReсordTest
    {
        [Fact]
        public void Insert_and_delete()
        {
            var fileManager = new FileManager("RerordTest", 400, new TestBlocksReadWriteTracker(), TestLoggerFactory.Instance, true);
            var logManager = new LogManager(fileManager, "log", TestLoggerFactory.Instance);
            var bufferManager = new BufferManager(fileManager, logManager, 3, TestLoggerFactory.Instance);
            var lockTable = new LockTable();
            Random random = new Random();

            Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable, TestLoggerFactory.Instance);

            Transaction tx = newTx();

            Schema sch = new Schema();
            sch.AddIntColumn("A");
            sch.AddStringColumn("B", 9);
            Layout layout = new Layout(sch);

            var fields = layout.schema().ColumnNames();
            fields[0].Should().Be("A");
            layout.offset("A").Should().Be(8);

            fields[1].Should().Be("B");
            layout.offset("B").Should().Be(12);

            BlockId blk = tx.append("testfile");
            tx.PinBlock(blk);
            RecordPage page = new RecordPage(tx, blk, layout);
            page.format();

            //Filling the page with random records
            int slot = page.insertAfter(-1);
            while (slot >= 0)
            {
                int n = random.Next(0, 49);
                page.setInt(slot, "A", n);
                page.setString(slot, "B", "rec" + n);
                slot = page.insertAfter(slot);
            }

            //Deleting these records, whose A-values are less than 25
            int deleteCount = 0;
            slot = page.nextAfter(-1);
            while (slot >= 0)
            {
                int a = page.getInt(slot, "A");
                String b = page.getString(slot, "B");
                if (a < 25)
                {
                    deleteCount++;
                    page.delete(slot);
                }
                slot = page.nextAfter(slot);
            }

            deleteCount.Should().BeGreaterThan(0);

            int remaining = 0;
            slot = page.nextAfter(-1);
            while (slot >= 0)
            {
                int a = page.getInt(slot, "A");
                a.Should().BeGreaterThanOrEqualTo(25);
                String b = page.getString(slot, "B");
                b.Should().Contain("rec");
                remaining++;
                slot = page.nextAfter(slot);
            }
            tx.UnpinBlock(blk);
            tx.Commit();

            remaining.Should().BeGreaterThan(0);
        }

        [Fact]
        public void Null_values_test()
        {
            var fileManager = new FileManager("RerordTestNullable", 400, new TestBlocksReadWriteTracker(), TestLoggerFactory.Instance, true);
            var logManager = new LogManager(fileManager, "log", TestLoggerFactory.Instance);
            var bufferManager = new BufferManager(fileManager, logManager, 3, TestLoggerFactory.Instance);
            var lockTable = new LockTable();
            Random random = new Random();

            Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable, TestLoggerFactory.Instance);

            Transaction tx = newTx();

            Schema sch = new Schema();
            sch.AddIntColumn("A", true);
            sch.AddStringColumn("B", 9, true);
            Layout layout = new Layout(sch);

            var fields = layout.schema().ColumnNames();
            fields[0].Should().Be("A");
            layout.offset("A").Should().Be(8);

            fields[1].Should().Be("B");
            layout.offset("B").Should().Be(12);

            BlockId blk = tx.append("testfile");
            tx.PinBlock(blk);
            RecordPage page = new RecordPage(tx, blk, layout);
            page.format();

            //Filling the page with random records
            int slot = page.insertAfter(-1);
            while (slot >= 0)
            {
                int n = random.Next(0, 49);
                page.setInt(slot, "A", n);
                if(n % 2 == 0)
                    page.setNull(slot, "B");
                else
                    page.setString(slot, "B", "rec" + n);
                slot = page.insertAfter(slot);
            }

            //Deleting these records, whose A-values are less than 25
            int deleteCount = 0;
            slot = page.nextAfter(-1);
            while (slot >= 0)
            {
                int a = page.getInt(slot, "A");
                String b = page.getString(slot, "B");
                if (a < 25)
                {
                    deleteCount++;
                    page.delete(slot);
                }
                slot = page.nextAfter(slot);
            }

            deleteCount.Should().BeGreaterThan(0);

            int remaining = 0;
            slot = page.nextAfter(-1);
            while (slot >= 0)
            {
                int a = page.getInt(slot, "A");
                a.Should().BeGreaterThanOrEqualTo(25);
                String b = page.getString(slot, "B");
                if (a % 2 == 0)
                    page.isNull(slot, "B").Should().BeTrue();
                else
                    b.Should().Contain("rec");
                remaining++;
                slot = page.nextAfter(slot);
            }
            tx.UnpinBlock(blk);
            tx.Commit();

            remaining.Should().BeGreaterThan(0);
        }
    }
}
