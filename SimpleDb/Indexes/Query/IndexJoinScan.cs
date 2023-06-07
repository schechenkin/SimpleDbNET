using SimpleDb.Query;
using SimpleDb.Record;
using SimpleDb.Types;

namespace SimpleDb.Indexes.Query
{
    /**
     * The scan class corresponding to the indexjoin relational
     * algebra operator.
     * The code is very similar to that of ProductScan, 
     * which makes sense because an index join is essentially
     * the product of each LHS record with the matching RHS index records.
     * @author Edward Sciore
     */
    public class IndexJoinScan : Scan
    {
        private Scan lhs;
        private SimpleDb.Index idx;
        private String joinfield;
        private TableScan rhs;

        /**
         * Creates an index join scan for the specified LHS scan and 
         * RHS index.
         * @param lhs the LHS scan
         * @param idx the RHS index
         * @param joinfield the LHS field used for joining
         * @param rhs the RHS scan
         */
        public IndexJoinScan(Scan lhs, SimpleDb.Index idx, String joinfield, TableScan rhs)
        {
            this.lhs = lhs;
            this.idx = idx;
            this.joinfield = joinfield;
            this.rhs = rhs;
            BeforeFirst();
        }

        /**
         * Positions the scan before the first record.
         * That is, the LHS scan will be positioned at its
         * first record, and the index will be positioned
         * before the first record for the join value.
         * @see simpledb.query.Scan#beforeFirst()
         */
        public void BeforeFirst()
        {
            lhs.BeforeFirst();
            lhs.Next();
            resetIndex();
        }

        /**
         * Moves the scan to the next record.
         * The method moves to the next index record, if possible.
         * Otherwise, it moves to the next LHS record and the
         * first index record.
         * If there are no more LHS records, the method returns false.
         * @see simpledb.query.Scan#next()
         */
        public bool Next()
        {
            while (true)
            {
                if (idx.next())
                {
                    rhs.MoveToRid(idx.getDataRid());
                    return true;
                }
                if (!lhs.Next())
                    return false;
                resetIndex();
            }
        }

        /**
         * Returns the integer value of the specified field.
         * @see simpledb.query.Scan#getVal(java.lang.String)
         */
        public int GetInt(string fldname)
        {
            if (rhs.HasField(fldname))
                return rhs.GetInt(fldname);
            else
                return lhs.GetInt(fldname);
        }

        /**
         * Returns the Constant value of the specified field.
         * @see simpledb.query.Scan#getVal(java.lang.String)
         */
        public Constant GetValue(string fldname)
        {
            if (rhs.HasField(fldname))
                return rhs.GetValue(fldname);
            else
                return lhs.GetValue(fldname);
        }

        /**
         * Returns the string value of the specified field.
         * @see simpledb.query.Scan#getVal(java.lang.String)
         */
        public DbString GetString(string fldname)
        {
            if (rhs.HasField(fldname))
                return rhs.GetString(fldname);
            else
                return lhs.GetString(fldname);
        }

        /** Returns true if the field is in the schema.
          * @see simpledb.query.Scan#hasField(java.lang.String)
          */
        public bool HasField(string fldname)
        {
            return rhs.HasField(fldname) || lhs.HasField(fldname);
        }

        /**
         * Closes the scan by closing its LHS scan and its RHS index.
         * @see simpledb.query.Scan#close()
         */
        public void Close()
        {
            lhs.Close();
            idx.close();
            rhs.Close();
        }

        private void resetIndex()
        {
            Constant searchkey = lhs.GetValue(joinfield);
            idx.beforeFirst(searchkey);
        }

        public DateTime GetDateTime(string fldname)
        {
            if (rhs.HasField(fldname))
                return rhs.GetDateTime(fldname);
            else
                return lhs.GetDateTime(fldname);
        }

        public bool IsNull(string fldname)
        {
            if (rhs.HasField(fldname))
                return rhs.IsNull(fldname);
            else
                return lhs.IsNull(fldname);
        }
    }
}
