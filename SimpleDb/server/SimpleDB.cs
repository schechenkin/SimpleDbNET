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
        public static int BLOCK_SIZE = 400;
        public static int BUFFER_SIZE = 8;
        public static string LOG_FILE = "simpledb.log";

        private FileMgr fm;
        private LogMgr lm;
        private BufferMgr bm;
        private MetadataMgr mdm;
        private Planner mPlanner;

        /**
         * A constructor useful for debugging.
         * @param dirname the name of the database directory
         * @param blocksize the block size
         * @param buffsize the number of buffers
         */
        public Server(string dirname, int blocksize, int buffsize, bool recreate = false)
        {
            fm = new FileMgr(dirname, blocksize, recreate);
            lm = new LogMgr(fm, LOG_FILE);
            bm = new BufferMgr(fm, lm, buffsize);
        }

        /**
         * A simpler constructor for most situations. Unlike the
         * 3-arg constructor, it also initializes the metadata tables.
         * @param dirname the name of the database directory
         */
        public Server(string dirname, bool recreate = false)
            :this(dirname, BLOCK_SIZE, BUFFER_SIZE, recreate)
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
            QueryPlanner qp = new BasicQueryPlanner(mdm);
            UpdatePlanner up = new BasicUpdatePlanner(mdm);
            //    QueryPlanner qp = new HeuristicQueryPlanner(mdm);
            //    UpdatePlanner up = new IndexUpdatePlanner(mdm);
            mPlanner = new Planner(qp, up);
        }

        public Transaction newTx()
        {
            return new Transaction(fm, lm, bm);
        }


        // These methods aid in debugging
        public FileMgr fileMgr()
        {
            return fm;
        }

        public LogMgr logMgr()
        {
            return lm;
        }

        public BufferMgr bufferMgr()
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
