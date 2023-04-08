using SimpleDb.Record;
using SimpleDb.Transactions;
using SimpleDB.Metadata;
using System;
using System.Collections.Generic;

namespace SimpleDb.Metadata
{
    public class MetadataMgr
    {
        private TableMgr tblmgr;
        private ViewMgr viewmgr;
        private StatMgr statmgr;
        //private static IndexMgr idxmgr;

        public MetadataMgr(bool isnew, Transaction tx)
        {
            tblmgr = new TableMgr(isnew, tx);
            viewmgr = new ViewMgr(isnew, tblmgr, tx);
            statmgr = new StatMgr(tblmgr, tx);
            //idxmgr = new IndexMgr(isnew, tblmgr, statmgr, tx);
        }

        public void createTable(string tblname, Schema sch, Transaction tx)
        {
            tblmgr.createTable(tblname, sch, tx);
        }

        public Layout getLayout(string tblname, Transaction tx)
        {
            return tblmgr.getLayout(tblname, tx);
        }

        public void createView(string viewname, string viewdef, Transaction tx)
        {
            viewmgr.createView(viewname, viewdef, tx);
        }

        public String? getViewDef(string viewname, Transaction tx)
        {
            return viewmgr.getViewDef(viewname, tx);
        }

        /*public void createIndex(string idxname, string tblname, string fldname, Transaction tx)
        {
            idxmgr.createIndex(idxname, tblname, fldname, tx);
        }

        public Dictionary<string, IndexInfo> getIndexInfo(string tblname, Transaction tx)
        {
            return idxmgr.getIndexInfo(tblname, tx);
        }*/

        public StatInfo getStatInfo(String tblname, Layout layout, Transaction tx)
        {
            return statmgr.getStatInfo(tblname, layout, tx);
        }
    }
}
