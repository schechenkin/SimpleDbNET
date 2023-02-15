using SimpleDb.Query;
using SimpleDB.file;
using SimpleDB.Query;
using SimpleDB.Record;

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
        private SimpleDB.Indexes.Index idx;
        private Constant val;

        /**
         * Creates an index select scan for the specified
         * index and selection constant.
         * @param idx the index
         * @param val the selection constant
         */
        public IndexSelectScan(TableScan ts, SimpleDB.Indexes.Index idx, Constant val)
        {
            this.ts = ts;
            this.idx = idx;
            this.val = val;
            beforeFirst();
        }

        /**
         * Positions the scan before the first record,
         * which in this case means positioning the index
         * before the first instance of the selection constant.
         * @see simpledb.query.Scan#beforeFirst()
         */
        public void beforeFirst()
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
        public bool next()
        {
            bool ok = idx.next();
            if (ok)
            {
                RID rid = idx.getDataRid();
                ts.moveToRid(rid);
            }
            return ok;
        }

        /**
         * Returns the value of the field of the current data record.
         * @see simpledb.query.Scan#getInt(java.lang.String)
         */
        public int getInt(string fldname)
        {
            return ts.getInt(fldname);
        }

        /**
         * Returns the value of the field of the current data record.
         * @see simpledb.query.Scan#getString(java.lang.String)
         */
        public String getString(string fldname)
        {
            return ts.getString(fldname);
        }

        /**
         * Returns the value of the field of the current data record.
         * @see simpledb.query.Scan#getVal(java.lang.String)
         */
        public Constant getVal(string fldname)
        {
            return ts.getVal(fldname);
        }

        /**
         * Returns whether the data record has the specified field.
         * @see simpledb.query.Scan#hasField(java.lang.String)
         */
        public bool hasField(string fldname)
        {
            return ts.hasField(fldname);
        }

        /**
         * Closes the scan by closing the index and the tablescan.
         * @see simpledb.query.Scan#close()
         */
        public void close()
        {
            idx.close();
            ts.close();
        }

        public bool CompareString(string fldname, StringConstant val)
        {
            return ts.CompareString(fldname, val);
        }

        public DateTime getDateTime(string fldname)
        {
            return ts.getDateTime(fldname);
        }

        public bool isNull(string fldname)
        {
            return ts.isNull(fldname);
        }

        public ConstantRefStruct getVal2(string fldname)
        {
            return ts.getVal2(fldname);
        }
    }
}
