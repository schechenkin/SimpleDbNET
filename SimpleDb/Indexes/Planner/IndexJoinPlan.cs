using SimpleDb.Indexes.Query;
using SimpleDB.Metadata;
using SimpleDB.Plan;
using SimpleDB.Query;
using SimpleDB.Record;

namespace SimpleDb.Indexes.Planner
{
    public class IndexJoinPlan : Plan
    {
        private Plan p1, p2;
        private IndexInfo rightHandIndexInfo;
        private string joinfield;
        private Schema sch = new Schema();

        /**
         * Implements the join operator,
         * using the specified LHS and RHS plans.
         * @param p1 the left-hand plan
         * @param p2 the right-hand plan
         * @param ii information about the right-hand index
         * @param joinfield the left-hand field used for joining
         */
        public IndexJoinPlan(Plan p1, Plan p2, IndexInfo rightHandIndexInfo, String joinfield)
        {
            this.p1 = p1;
            this.p2 = p2;
            this.rightHandIndexInfo = rightHandIndexInfo;
            this.joinfield = joinfield;
            sch.AddAll(p1.schema());
            sch.AddAll(p2.schema());
        }

        /**
         * Opens an indexjoin scan for this query
         * @see simpledb.plan.Plan#open()
         */
        public Scan open()
        {
            Scan s = p1.open();
            // throws an exception if p2 is not a tableplan
            TableScan ts = (TableScan)p2.open();
            var idx = rightHandIndexInfo.open();
            return new IndexJoinScan(s, idx, joinfield, ts);
        }

        /**
         * Estimates the number of block accesses to compute the join.
         * The formula is:
         * <pre> B(indexjoin(p1,p2,idx)) = B(p1) + R(p1)*B(idx)
         *       + R(indexjoin(p1,p2,idx) </pre>
         * @see simpledb.plan.Plan#blocksAccessed()
         */
        public int blocksAccessed()
        {
            return p1.blocksAccessed()
               + (p1.recordsOutput() * rightHandIndexInfo.blocksAccessed())
               + recordsOutput();
        }

        /**
         * Estimates the number of output records in the join.
         * The formula is:
         * <pre> R(indexjoin(p1,p2,idx)) = R(p1)*R(idx) </pre>
         * @see simpledb.plan.Plan#recordsOutput()
         */
        public int recordsOutput()
        {
            return p1.recordsOutput() * rightHandIndexInfo.recordsOutput();
        }

        /**
         * Estimates the number of distinct values for the 
         * specified field.  
         * @see simpledb.plan.Plan#distinctValues(java.lang.String)
         */
        public int distinctValues(String fldname)
        {
            if (p1.schema().HasField(fldname))
                return p1.distinctValues(fldname);
            else
                return p2.distinctValues(fldname);
        }

        /**
         * Returns the schema of the index join.
         * @see simpledb.plan.Plan#schema()
         */
        public Schema schema()
        {
            return sch;
        }
    }
}
