using SimpleDb.Record;
using SimpleDb.Transactions;
using SimpleDb.Types;
using SimpleDB.Metadata;

namespace SimpleDb.Metadata;

class IndexMgr
    {
        private Layout layout;
        private TableMgr tblmgr;
        private StatMgr statmgr;

        /**
         * Create the index manager.
         * This constructor is called during system startup.
         * If the database is new, then the <i>idxcat</i> table is created.
         * @param isnew indicates whether this is a new database
         * @param tx the system startup transaction
         */
        public IndexMgr(bool isnew, TableMgr tblmgr, StatMgr statmgr, Transaction tx)
        {
            if (isnew)
            {
                Schema sch = new Schema();
                sch.AddStringColumn("indexname", TableMgr.MAX_NAME);
                sch.AddStringColumn("tablename", TableMgr.MAX_NAME);
                sch.AddStringColumn("fieldname", TableMgr.MAX_NAME);
                tblmgr.createTable("idxcat", sch, tx);
            }
            this.tblmgr = tblmgr;
            this.statmgr = statmgr;
            layout = tblmgr.getLayout("idxcat", tx);
        }

        /**
         * Create an index of the specified type for the specified field.
         * A unique ID is assigned to this index, and its information
         * is stored in the idxcat table.
         * @param idxname the name of the index
         * @param tblname the name of the indexed table
         * @param fldname the name of the indexed field
         * @param tx the calling transaction
         */
        public void createIndex(String idxname, String tblname, String fldname, Transaction tx)
        {
            TableScan ts = new TableScan(tx, "idxcat", layout);
            ts.Insert();
            ts.SetValue("indexname", (DbString)idxname);
            ts.SetValue("tablename", (DbString)tblname);
            ts.SetValue("fieldname", (DbString)fldname);
            ts.Close();
        }

        /**
         * Return a map containing the index info for all indexes
         * on the specified table.
         * @param tblname the name of the table
         * @param tx the calling transaction
         * @return a map of IndexInfo objects, keyed by their field names
         */
        public Dictionary<string, IndexInfo> getIndexInfo(String tblname, Transaction tx)
        {
            Dictionary<string, IndexInfo> result = new ();
            TableScan ts = new TableScan(tx, "idxcat", layout);
            while (ts.Next())
                if (ts.GetString("tablename").Equals(tblname))
                {
                    DbString idxname = ts.GetString("indexname");
                    DbString fldname = ts.GetString("fieldname");
                    Layout tblLayout = tblmgr.getLayout(tblname, tx);
                    StatInfo tblsi = statmgr.getStatInfo(tblname, tblLayout, tx);
                    IndexInfo ii = new IndexInfo(idxname, fldname, tblLayout.schema(), tx, tblsi);
                    result[fldname.GetString()] = ii;
                }
            ts.Close();
            return result;
        }
    }
