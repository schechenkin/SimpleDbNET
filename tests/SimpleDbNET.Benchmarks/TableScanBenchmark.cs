﻿using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using QueryPlannerTest;
using SimpleDb.Transactions.Concurrency;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Metadata;
using SimpleDB.Record;
using SimpleDB.Tx;

namespace SimpleDbNET.Benchmarks
{
    [SimpleJob(RuntimeMoniker.Net60)]
    [MemoryDiagnoser]
    public class TableScanBenchmark
    {
        FileManager fileManager;
        LogManager logManager;
        BufferManager bufferManager;
        LockTable lockTable;
        Random random = new Random();

        TableScan tableScan;
        Layout layout;
        StringConstant stringConstant = new StringConstant("rec10");

        private Transaction newTx()
        {
            return new Transaction(fileManager, logManager, bufferManager, lockTable);
        }

        [GlobalSetup]
        public void Configure()
        {
            fileManager = new FileManager("TableScanBenchmark", 4096, new EmptyBlocksReadWriteTracker(), true);
            logManager = new LogManager(fileManager, "log");
            bufferManager = new BufferManager(fileManager, logManager, 1024);
            lockTable = new LockTable();

            var tx = newTx();

            MetadataMgr mdm = new MetadataMgr(true, tx);

            Schema sch = new Schema();
            sch.AddIntColumn("A");
            sch.AddStringColumn("B", 9);

            //Fill table
            mdm.createTable("MyTable", sch, tx);
            layout = mdm.getLayout("MyTable", tx);

            TableScan ts = new TableScan(tx, "MyTable", layout);
            for (int i = 0; i < 1000; i++)
            {
                ts.insert();
                int n = random.Next(0, 100);
                ts.setInt("A", n);
                ts.setString("B", "rec" + n);
            }

            tx.Commit();

            var transaction = newTx();
            tableScan = new TableScan(transaction, "MyTable", layout);
        }

        //[Benchmark]
        public TableScan beforeFirst()
        {
            tableScan.beforeFirst();
            return tableScan;
        }

        //[Benchmark]
        public TableScan newTableScan()
        {
            return new TableScan(newTx(), "MyTable", layout);
        }

        //[Benchmark]
        public TableScan TableScanClose()
        {
            var tableScan = new TableScan(newTx(), "MyTable", layout);
            tableScan.close();

            return tableScan;
        }

        [Benchmark]
        public int FullScan()
        {
            int counter = 0;
            
            tableScan.beforeFirst();
            while (tableScan.next())
            {
                var a = tableScan.getInt("A");
                if(a > 50)
                    counter++;
            }

            return counter;
        }

        [Benchmark]
        public int FullScanWithString()
        {
            int counter = 0;

            tableScan.beforeFirst();
            while (tableScan.next())
            {
                if (tableScan.getString("B").Equals("rec10"))
                    counter++;
            }

            return counter;
        }

        [Benchmark]
        public int FullScanWithStringCompare()
        {
            int counter = 0;

            tableScan.beforeFirst();
            while (tableScan.next())
            {
                if (tableScan.CompareString("B", stringConstant))
                    counter++;
            }

            return counter;
        }
    }
}
