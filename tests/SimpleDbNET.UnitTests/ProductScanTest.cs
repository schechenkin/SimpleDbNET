using FluentAssertions;
using Microsoft.Extensions.Logging;
using SimpleDb.Transactions.Concurrency;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Query;
using SimpleDB.Record;
using SimpleDB.Tx;
using SimpleDbNET.UnitTests.Fixtures;
using Xunit;


namespace SimpleDbNET.UnitTests
{
    public class ScanTests
    {

        [Fact]
        public void ProductScanTest()
        {
            var fileManager = new FileManager("ProductScanTest", 400, new TestBlocksReadWriteTracker(), TestLoggerFactory.Instance, true);
            var logManager = new LogManager(fileManager, "log", TestLoggerFactory.Instance);
            var bufferManager = new BufferManager(fileManager, logManager, 3, TestLoggerFactory.Instance);
            var lockTable = new LockTable();

            Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable, TestLoggerFactory.Instance);

            Transaction tx = newTx();

            //give 2 tables
            Schema sch1 = new Schema();
            sch1.AddIntColumn("A");
            sch1.AddStringColumn("B", 9);
            Layout layout1 = new Layout(sch1);
            TableScan ts1 = new TableScan(tx, "T1", layout1);

            Schema sch2 = new Schema();
            sch2.AddIntColumn("C");
            sch2.AddStringColumn("D", 9);
            Layout layout2 = new Layout(sch2);
            TableScan ts2 = new TableScan(tx, "T2", layout2);

            ts1.beforeFirst();
            int n = 2;
            for (int i = 0; i < n; i++)
            {
                ts1.insert();
                ts1.setInt("A", i);
                ts1.setString("B", "aaa" + i);
            }
            ts1.close();

            ts2.beforeFirst();
            for (int i = 0; i < n; i++)
            {
                ts2.insert();
                ts2.setInt("C", i);
                ts2.setString("D", "bbb" + i);
            }
            ts2.close();

            Scan s1 = new TableScan(tx, "T1", layout1);
            Scan s2 = new TableScan(tx, "T2", layout2);

            //when go throught ProductScan
            Scan s3 = new ProductScan(s1, s2);

            //then 1st row
            s3.next().Should().BeTrue();
            s3.getInt("A").Should().Be(0);
            s3.getString("B").Should().Be("aaa0");
            s3.getInt("C").Should().Be(0);
            s3.getString("D").Should().Be("bbb0");

            //then 2nd row
            s3.next().Should().BeTrue();
            s3.getInt("A").Should().Be(0);
            s3.getString("B").Should().Be("aaa0");
            s3.getInt("C").Should().Be(1);
            s3.getString("D").Should().Be("bbb1");

            //then 3rd row
            s3.next().Should().BeTrue();
            s3.getInt("A").Should().Be(1);
            s3.getString("B").Should().Be("aaa1");
            s3.getInt("C").Should().Be(0);
            s3.getString("D").Should().Be("bbb0");

            //then 4th row
            s3.next().Should().BeTrue();
            s3.getInt("A").Should().Be(1);
            s3.getString("B").Should().Be("aaa1");
            s3.getInt("C").Should().Be(1);
            s3.getString("D").Should().Be("bbb1");

            s3.next().Should().BeFalse();

            s3.close();
            tx.Commit();
        }

        [Fact]
        public void SelectScanTest()
        {
            var fileManager = new FileManager("SelectScanTest", 400, new TestBlocksReadWriteTracker(), TestLoggerFactory.Instance, true);
            var logManager = new LogManager(fileManager, "log", TestLoggerFactory.Instance);
            var bufferManager = new BufferManager(fileManager, logManager, 3, TestLoggerFactory.Instance);
            var lockTable = new LockTable();

            Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable, TestLoggerFactory.Instance);

            Transaction tx = newTx();

            Schema sch1 = new Schema();
            sch1.AddIntColumn("A");
            sch1.AddStringColumn("B", 9);
            Layout layout = new Layout(sch1);

            //Fill table with 20 records
            UpdateScan s1 = new TableScan(tx, "T", layout);
            s1.beforeFirst();
            int n = 20;
            for (int i = 0; i < n; i++)
            {
                s1.insert();
                s1.setInt("A", i);
                s1.setString("B", "rec" + i);
            }
            s1.close();

            // selecting all records where A=10
            Scan tableScan = new TableScan(tx, "T", layout);
            Constant c = new Constant(10);
            Term t = new Term(new Expression("A"), new Expression(c));
            Predicate pred = new Predicate(t);
            pred.ToString().Should().Be("A=10");

            //should contain single record
            Scan s3 = new SelectScan(tableScan, pred);
            s3.next().Should().BeTrue();
            s3.getInt("A").Should().Be(10);
            s3.getString("B").Should().Be("rec10");

            s3.next().Should().BeFalse();

            // selecting all records where A>10
            Scan s4 = new TableScan(tx, "T", layout);
            t = new Term(new Expression("A"), new Expression(c), Term.CompareOperator.More);
            pred = new Predicate(t);
            pred.ToString().Should().Be("A>10");

            //should contain 10 records
            Scan s5 = new SelectScan(s4, pred);
            int j = 11;
            while(j < 20)
            {
                s5.next().Should().BeTrue();
                s5.getInt("A").Should().Be(j);
                s5.getString("B").Should().Be($"rec{j}");
                j++;
            }
            s5.next().Should().BeFalse();
        }

        [Fact]
        public void ProjectScanTest()
        {
            var fileManager = new FileManager("ProjectScanTest", 400, new TestBlocksReadWriteTracker(), TestLoggerFactory.Instance, true);
            var logManager = new LogManager(fileManager, "log", TestLoggerFactory.Instance);
            var bufferManager = new BufferManager(fileManager, logManager, 3, TestLoggerFactory.Instance);
            var lockTable = new LockTable();

            Func<Transaction> newTx = () => new Transaction(fileManager, logManager, bufferManager, lockTable, TestLoggerFactory.Instance);

            Transaction tx = newTx();

            Schema sch1 = new Schema();
            sch1.AddIntColumn("A");
            sch1.AddStringColumn("B", 9);
            Layout layout = new Layout(sch1);

            //Fill table with 10 records
            UpdateScan s1 = new TableScan(tx, "T", layout);
            s1.beforeFirst();
            int n = 10;
            for (int i = 0; i < n; i++)
            {
                s1.insert();
                s1.setInt("A", i);
                s1.setString("B", "rec" + i);
            }
            s1.close();

            // selecting all records where A=5
            Scan s2 = new TableScan(tx, "T", layout);
            Constant c = new Constant(5);
            Term t = new Term(new Expression("A"), new Expression(c));
            Predicate pred = new Predicate(t);


            Scan s3 = new SelectScan(s2, pred);
            //should contain single record
            Scan s4 = new ProjectScan(s3, new() { "B" });
            s4.next().Should().BeTrue();
            s4.getString("B").Should().Be("rec5");
            s4.hasField("A").Should().BeFalse();

            s4.next().Should().BeFalse();
        }
    }
}
