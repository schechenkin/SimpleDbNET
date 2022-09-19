using FluentAssertions;
using SimpleDb.Transactions.Concurrency;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Record;
using SimpleDB.Tx;
using Xunit;

namespace SimpleDbNET.UnitTests
{
    public class TableScanTest
    {
        [Fact]
        public void Insert_and_delete()
        {
            var fileManager = new FileManager("TableScanTest", 400, new TestBlocksReadWriteTracker(), true);
            var logManager = new LogManager(fileManager, "log");
            var bufferManager = new BufferManager(fileManager, logManager, 3);
            var lockTable = new LockTable();
            Random random = new Random();

            Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable);
            
            Transaction tx = newTx();

            Schema sch = new Schema();
            sch.AddIntColumn("A");
            sch.AddStringColumn("B", 9);
            Layout layout = new Layout(sch);

            //Filling the table with 50 random records
            TableScan tableScan = new TableScan(tx, "T", layout);
            for (int i = 0; i < 50; i++)
            {
                tableScan.insert();
                int n = random.Next(0, 49);
                tableScan.setInt("A", n);
                tableScan.setString("B", "rec" + n);
            }

            //Deleting these records, whose A-values are less than 25
            int deleteCount = 0;
            tableScan.beforeFirst();
            while (tableScan.next())
            {
                int a = tableScan.getInt("A");
                if (a < 25)
                {
                    deleteCount++;
                    tableScan.delete();
                }
            }
            deleteCount.Should().BeGreaterThan(0);

            //check remaining records
            int remaining = 0;
            tableScan.beforeFirst();
            while (tableScan.next())
            {
                tableScan.getInt("A").Should().BeGreaterThan(0);
                tableScan.getString("B").Should().NotBeNullOrEmpty();
                //tableScan.getRid().Should().NotBeNull();
                remaining++;
            }
            tableScan.close();
            tx.Commit();

            remaining.Should().BeGreaterThan(0);
        }
    }
}
