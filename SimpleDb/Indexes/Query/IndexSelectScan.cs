using SimpleDb.Query;
using SimpleDb.Query;
using SimpleDb.Record;
using SimpleDb.Types;

namespace SimpleDb.Indexes.Query
{
    /**
     * The scan class corresponding to the select relational
     * algebra operator.
     * @author Edward Sciore
     */
    public class IndexSelectScan : Scan
    {
        private TableScan ts;
        private SimpleDb.Index idx;
        private Constant val;

        /**
         * Creates an index select scan for the specified
         * index and selection constant.
         * @param idx the index
         * @param val the selection constant
         */
        public IndexSelectScan(TableScan ts, SimpleDb.Index idx, Constant val)
        {
            this.ts = ts;
            this.idx = idx;
            this.val = val;
            BeforeFirst();
        }

        /**
         * Positions the scan before the first record,
         * which in this case means positioning the index
         * before the first instance of the selection constant.
         * @see simpledb.query.Scan#beforeFirst()
         */
        public void BeforeFirst()
        {
            idx.beforeFirst(val);
        }

        /**
         * Moves to the next record, which in this case means
         * moving the index to the next record satisfying the
         * selection constant, and returning false if there are
         * no more such index records.
         * If there is a next record, the method moves the 
         * tablescan to the corresponding data record.
         * @see simpledb.query.Scan#next()
         */
        public bool Next()
        {
            bool ok = idx.next();
            if (ok)
            {
                RID rid = idx.getDataRid();
                ts.MoveToRid(rid);
            }
            return ok;
        }

        /**
         * Returns the value of the field of the current data record.
         * @see simpledb.query.Scan#getInt(java.lang.String)
         */
        public int GetInt(string fldname)
        {
            return ts.GetInt(fldname);
        }

        /**
         * Returns the value of the field of the current data record.
         * @see simpledb.query.Scan#getString(java.lang.String)
         */
        public DbString GetString(string fldname)
        {
            return ts.GetString(fldname);
        }

        /**
         * Returns the value of the field of the current data record.
         * @see simpledb.query.Scan#getVal(java.lang.String)
         */
        public Constant GetValue(string fldname)
        {
            return ts.GetValue(fldname);
        }

        /**
         * Returns whether the data record has the specified field.
         * @see simpledb.query.Scan#hasField(java.lang.String)
         */
        public bool HasField(string fldname)
        {
            return ts.HasField(fldname);
        }

        /**
         * Closes the scan by closing the index and the tablescan.
         * @see simpledb.query.Scan#close()
         */
        public void Close()
        {
            idx.close();
            ts.Close();
        }

        public DateTime GetDateTime(string fldname)
        {
            return ts.GetDateTime(fldname);
        }

        public bool IsNull(string fldname)
        {
            return ts.IsNull(fldname);
        }
    }
}
