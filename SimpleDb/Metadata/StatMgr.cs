using SimpleDB.Record;
using SimpleDB.Tx;
using System;
using System.Collections.Generic;
using System.Threading;

namespace SimpleDB.Metadata
{
    class StatMgr
    {
        private TableMgr tblMgr;
        private Dictionary<string, StatInfo> tablestats = new();
        private int numcalls;
        private Mutex mutex = new Mutex();

        /**
         * Create the statistics manager.
         * The initial statistics are calculated by
         * traversing the entire database.
         * @param tx the startup transaction
         */
        public StatMgr(TableMgr tblMgr, Transaction tx)
        {
            this.tblMgr = tblMgr;
            refreshStatistics(tx);

            Console.WriteLine("tables stats:");
            foreach(var tableStat in tablestats)
            {
                Console.WriteLine($"table {tableStat.Key}, records: {tableStat.Value.recordsOutput()} blocks: {tableStat.Value.blocksAccessed()}");
            }
        }

        /**
         * Return the statistical information about the specified table.
         * @param tblname the name of the table
         * @param layout the table's layout
         * @param tx the calling transaction
         * @return the statistical information about the table
         */
        public StatInfo getStatInfo(string tblname, Layout layout, Transaction tx)
        {
            lock(mutex)
            {
                /*numcalls++;
                if (numcalls > 100)
                    refreshStatistics(tx);*/
                if (!tablestats.ContainsKey(tblname))
                {
                    tablestats[tblname] = calcTableStats(tblname, layout, tx);
                }
                return tablestats[tblname];
            }
        }

        private void refreshStatistics(Transaction tx)
        {
            Console.WriteLine($"refreshStatistics");

            tablestats = new ();
            numcalls = 0;
            Layout tcatlayout = tblMgr.getLayout("tblcat", tx);
            TableScan tcat = new TableScan(tx, "tblcat", tcatlayout);
            while (tcat.next())
            {
                string tblname = tcat.getString("tblname");
                Layout layout = tblMgr.getLayout(tblname, tx);
                StatInfo si = calcTableStats(tblname, layout, tx);
                tablestats[tblname] = si;
            }
            tcat.close();
        }

        private StatInfo calcTableStats(string tblname, Layout layout, Transaction tx)
        {
            int numRecs = 0;
            int numblocks = 0;
            /*TableScan ts = new TableScan(tx, tblname, layout);
            while (ts.next())
            {
                numRecs++;
                numblocks = ts.getRid().blockNumber() + 1;
            }
            ts.close();*/
            return new StatInfo(numblocks, numRecs);
        }
    }
}
