﻿using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDb.Transactions.Concurrency;
using SimpleDB.Tx;
using FluentAssertions;

namespace Playground
{
    internal class RevertNotCommitedChangesTest
    {
        public void Run()
        {
            WriteNotCommitedChangesToDisk();
            TryRecover();
            ReadValues();
        }

        private void ReadValues()
        {
            var fileManager = new FileManager("TransactionTest", 256, new EmptyBlocksReadWriteTracker());
            var logManager = new LogManager(fileManager, "log");
            var bufferManager = new BufferManager(fileManager, logManager, 3);
            var lockTable = new LockTable();

            BlockId blk = BlockId.New("testfile", 0);

            Transaction tx = new Transaction(fileManager, logManager, bufferManager, lockTable);
            tx.PinBlock(blk);
            tx.GetInt(blk, 0).Should().Be(78);
            tx.GetString(blk, 4).Should().Be("one");
            tx.Commit();

            fileManager.CloseFiles();
        }

        private void TryRecover()
        {
            var fileManager = new FileManager("TransactionTest", 256, new EmptyBlocksReadWriteTracker());
            var logManager = new LogManager(fileManager, "log");
            var bufferManager = new BufferManager(fileManager, logManager, 3);
            var lockTable = new LockTable();

            logManager.Print();

            Transaction tx = new Transaction(fileManager, logManager, bufferManager, lockTable);
            tx.Recover();

            logManager.Print();

            fileManager.CloseFiles();
        }

        private static void WriteNotCommitedChangesToDisk()
        {
            var fileManager = new FileManager("TransactionTest", 256, new EmptyBlocksReadWriteTracker(), true);
            var logManager = new LogManager(fileManager, "log");
            var bufferManager = new BufferManager(fileManager, logManager, 3);
            var lockTable = new LockTable();

            bufferManager.Print();
            logManager.Print();

            //Save some values to 1st block
            Transaction tx1 = new Transaction(fileManager, logManager, bufferManager, lockTable);
            BlockId blk = BlockId.New("testfile", 0);
            tx1.PinBlock(blk);
            tx1.SetInt(blk, 0, 78, true);
            tx1.SetString(blk, 4, "one", true);
            tx1.Commit();

            logManager.Print();

            Transaction tx2 = new Transaction(fileManager, logManager, bufferManager, lockTable);
            blk = BlockId.New("testfile", 0);
            tx2.PinBlock(blk);
            tx2.SetInt(blk, 0, 79, true);
            tx2.SetString(blk, 4, "two", true);

            var buffer = tx2.GetBuffer(blk);
            buffer.Flush();

            //bufferManager.Print();
            logManager.Print();
            logManager.Flush();

            fileManager.CloseFiles();
        }
    }
}
