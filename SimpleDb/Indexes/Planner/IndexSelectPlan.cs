using SimpleDb.Indexes.Query;
using SimpleDB.Metadata;
using SimpleDB.Plan;
using SimpleDB.Query;
using SimpleDB.Record;

namespace SimpleDb.Indexes.Planner
{
    public class IndexSelectPlan : Plan
    {
        private Plan plan;
        private IndexInfo indexInfo;
        private Constant val;

        /**
         * Creates a new indexselect node in the query tree
         * for the specified index and selection constant.
         * @param p the input table
         * @param ii information about the index
         * @param val the selection constant
         * @param tx the calling transaction 
         */
        public IndexSelectPlan(Plan p, IndexInfo ii, Constant val)
        {
            this.plan = p;
            this.indexInfo = ii;
            this.val = val;
        }

        /** 
         * Creates a new indexselect scan for this query
         * @see simpledb.plan.Plan#open()
         */
        public Scan open()
        {
            // throws an exception if p is not a tableplan.
            TableScan ts = (TableScan)plan.open();
            var idx = indexInfo.open();
            return new IndexSelectScan(ts, idx, val);
        }

        /**
         * Estimates the number of block accesses to compute the 
         * index selection, which is the same as the 
         * index traversal cost plus the number of matching data records.
         * @see simpledb.plan.Plan#blocksAccessed()
         */
        public int blocksAccessed()
        {
            return indexInfo.blocksAccessed() + recordsOutput();
        }

        /**
         * Estimates the number of output records in the index selection,
         * which is the same as the number of search key values
         * for the index.
         * @see simpledb.plan.Plan#recordsOutput()
         */
        public int recordsOutput()
        {
            return indexInfo.recordsOutput();
        }

        /** 
         * Returns the distinct values as defined by the index.
         * @see simpledb.plan.Plan#distinctValues(java.lang.String)
         */
        public int distinctValues(string fldname)
        {
            return indexInfo.distinctValues(fldname);
        }

        /**
         * Returns the schema of the data table.
         * @see simpledb.plan.Plan#schema()
         */
        public Schema schema()
        {
            return plan.schema();
        }
    }
}
