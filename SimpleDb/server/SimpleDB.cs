using SimpleDb.file;
using SimpleDb.Transactions.Concurrency;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Metadata;
using SimpleDB.Plan;
using SimpleDB.Tx;
using System;

namespace SimpleDB
{
    public class Server
    {
        public static int BLOCK_SIZE = 4096;
        public static int BUFFER_SIZE = 1000*25*10;
        public static string LOG_FILE = "simpledb.log";

        private FileManager fm;
        private LogManager lm;
        private BufferManager bm;
        private MetadataMgr mdm;
        private Planner mPlanner;
        private LockTable lockTable;

        /**
         * A constructor useful for debugging.
         * @param dirname the name of the database directory
         * @param blocksize the block size
         * @param buffsize the number of buffers
         */
        public Server(string dirname, int blocksize, int buffsize, IBlocksReadWriteTracker blocksReadWriteTracker, bool recreate = false)
        {
            fm = new FileManager(dirname, blocksize, blocksReadWriteTracker, recreate, 262144);
            lm = new LogManager(fm, LOG_FILE);
            bm = new BufferManager(fm, lm, buffsize);
            lockTable = new LockTable();
        }

        /**
         * A simpler constructor for most situations. Unlike the
         * 3-arg constructor, it also initializes the metadata tables.
         * @param dirname the name of the database directory
         */
        public Server(string dirname, IBlocksReadWriteTracker blocksReadWriteTracker, bool recreate = false)
            :this(dirname, BLOCK_SIZE, BUFFER_SIZE, blocksReadWriteTracker, recreate)
        {
            Transaction tx = newTx();
            bool isnew = fm.IsNew();
            if (isnew)
                Console.WriteLine("creating new database");
            else
            {
                Console.WriteLine("recovering existing database");
            }
            mdm = new MetadataMgr(isnew, tx);

            tx.Commit();

            QueryPlanner qp = new BasicQueryPlanner(mdm);
            UpdatePlanner up = new BasicUpdatePlanner(mdm);
            //    QueryPlanner qp = new HeuristicQueryPlanner(mdm);
            //    UpdatePlanner up = new IndexUpdatePlanner(mdm);
            mPlanner = new Planner(qp, up);
        }

        public Transaction newTx()
        {
            return new Transaction(fm, lm, bm, lockTable);
        }


        // These methods aid in debugging
        public FileManager fileMgr()
        {
            return fm;
        }

        public LogManager logMgr()
        {
            return lm;
        }

        public BufferManager bufferMgr()
        {
            return bm;
        }

        public Planner planner()
        {
            return mPlanner;
        }

        public MetadataMgr mdMgr()
        {
            return mdm;
        }
    }

}
