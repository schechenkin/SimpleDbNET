using FluentAssertions;
using SimpleDb.Transactions.Concurrency;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Tx;
using Xunit;

namespace SimpleDbNET.UnitTests
{
    public class TransactionTest
    {
        [Fact]
        public void Commit_and_rollback_test()
        {
            var fileManager = new FileManager("TransactionTest", 400, new TestBlocksReadWriteTracker(), true);
            var logManager = new LogManager(fileManager, "log");
            var bufferManager = new BufferManager(fileManager, logManager, 3);
            var lockTable = new LockTable();

            //Save some values to 1st block
            Transaction tx1 = new Transaction(fileManager, logManager, bufferManager ,lockTable);
            BlockId blk = BlockId.New("testfile", 1);
            tx1.PinBlock(blk);
            tx1.SetInt(blk, 80, 1, false);
            tx1.SetString(blk, 40, "one", false);
            tx1.Commit();

            //Check values and update them
            Transaction tx2 = new Transaction(fileManager, logManager, bufferManager, lockTable);
            tx2.PinBlock(blk);
            tx2.GetInt(blk, 80).Should().Be(1);
            tx2.GetString(blk, 40).Should().Be("one");
            tx2.SetInt(blk, 80, 2, true);
            tx2.SetString(blk, 40, "two", true);
            tx2.Commit();

            //update value and rollback
            Transaction tx3 = new Transaction(fileManager, logManager, bufferManager, lockTable);
            tx3.PinBlock(blk);
            tx3.GetInt(blk, 80).Should().Be(2);
            tx3.GetString(blk, 40).Should().Be("two");
            //update int val
            tx3.SetInt(blk, 80, 9999, true);
            tx3.GetInt(blk, 80).Should().Be(9999);
            //rollback
            tx3.Rollback();

            //value should not be changed
            Transaction tx4 = new Transaction(fileManager, logManager, bufferManager, lockTable);
            tx4.PinBlock(blk);
            tx4.GetInt(blk, 80).Should().Be(2);
            tx4.Commit();
        }
    }
}
