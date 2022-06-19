using FluentAssertions;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Record;
using SimpleDB.Tx;
using Xunit;

namespace SimpleDbNET.UnitTests
{
    public class RerordTest
    {
        [Fact]
        public void Insert_and_delete()
        {
            var fileManager = new FileManager("RerordTest", 400, true);
            var logManager = new LogManager(fileManager, "log");
            var bufferManager = new BufferManager(fileManager, logManager, 3);
            Random random = new Random();

            Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager);

            Transaction tx = newTx();

            Schema sch = new Schema();
            sch.AddIntColumn("A");
            sch.AddStringColumn("B", 9);
            Layout layout = new Layout(sch);

            var fields = layout.schema().ColumnNames();
            fields[0].Should().Be("A");
            layout.offset("A").Should().Be(4);

            fields[1].Should().Be("B");
            layout.offset("B").Should().Be(8);

            BlockId blk = tx.append("testfile");
            tx.PinBlock(blk);
            RecordPage rp = new RecordPage(tx, blk, layout);
            rp.format();

            //Filling the page with random records
            int slot = rp.insertAfter(-1);
            while (slot >= 0)
            {
                int n = random.Next(0, 49);
                rp.setInt(slot, "A", n);
                rp.setString(slot, "B", "rec" + n);
                slot = rp.insertAfter(slot);
            }

            //Deleting these records, whose A-values are less than 25
            int deleteCount = 0;
            slot = rp.nextAfter(-1);
            while (slot >= 0)
            {
                int a = rp.getInt(slot, "A");
                String b = rp.getString(slot, "B");
                if (a < 25)
                {
                    deleteCount++;
                    rp.delete(slot);
                }
                slot = rp.nextAfter(slot);
            }

            deleteCount.Should().BeGreaterThan(0);

            int remaining = 0;
            slot = rp.nextAfter(-1);
            while (slot >= 0)
            {
                int a = rp.getInt(slot, "A");
                a.Should().BeGreaterThanOrEqualTo(25);
                String b = rp.getString(slot, "B");
                b.Should().Contain("rec");
                remaining++;
                slot = rp.nextAfter(slot);
            }
            tx.UnpinBlock(blk);
            tx.Commit();

            remaining.Should().BeGreaterThan(0);
        }
    }
}
