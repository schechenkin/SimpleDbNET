using SimpleDb.Metadata;
using SimpleDb.Query;
using SimpleDb.Record;
using SimpleDb.Transactions;
using System;

namespace SimpleDb.Plan
{
    internal class TablePlan : Plan
    {
        private string tblname;
        private Transaction tx;
        private Layout layout;
        private StatInfo si;

        /**
         * Creates a leaf node in the query tree corresponding
         * to the specified table.
         * @param tblname the name of the table
         * @param tx the calling transaction
         */
        public TablePlan(Transaction tx, string tblname, MetadataMgr md)
        {
            this.tblname = tblname;
            this.tx = tx;
            layout = md.getLayout(tblname, tx);
            si = md.getStatInfo(tblname, layout, tx);
        }

        public string tableName => tblname;

        /**
         * Creates a table scan for this query.
         * @see simpledb.plan.Plan#open()
         */
        public Scan open()
        {
            return new TableScan(tx, tblname, layout);
        }

        /**
         * Estimates the number of block accesses for the table,
         * which is obtainable from the statistics manager.
         * @see simpledb.plan.Plan#blocksAccessed()
         */
        public int blocksAccessed()
        {
            return si.blocksAccessed();
        }

        /**
         * Estimates the number of records in the table,
         * which is obtainable from the statistics manager.
         * @see simpledb.plan.Plan#recordsOutput()
         */
        public int recordsOutput()
        {
            return si.recordsOutput();
        }

        /**
         * Estimates the number of distinct field values in the table,
         * which is obtainable from the statistics manager.
         * @see simpledb.plan.Plan#distinctValues(java.lang.String)
         */
        public int distinctValues(string fldname)
        {
            return si.distinctValues(fldname);
        }

        /**
         * Determines the schema of the table,
         * which is obtainable from the catalog manager.
         * @see simpledb.plan.Plan#schema()
         */
        public Schema schema()
        {
            return layout.schema();
        }
    }

}
