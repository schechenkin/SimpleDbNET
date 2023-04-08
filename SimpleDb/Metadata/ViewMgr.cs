using SimpleDb.File;
using SimpleDb.Record;
using SimpleDb.Transactions;
using SimpleDb.Types;
using System;

namespace SimpleDB.Metadata
{
    class ViewMgr
    {
        // the max chars in a view definition.
        private static int MAX_VIEWDEF = 100;

        TableMgr tblMgr;

        public ViewMgr(bool isNew, TableMgr tblMgr, Transaction tx)
        {
            this.tblMgr = tblMgr;
            if (isNew)
            {
                Schema sch = new Schema();
                sch.AddStringColumn("viewname", TableMgr.MAX_NAME);
                sch.AddStringColumn("viewdef", MAX_VIEWDEF);
                tblMgr.createTable("viewcat", sch, tx);
            }
        }

        public void createView(String vname, String vdef, Transaction tx)
        {
            Layout layout = tblMgr.getLayout("viewcat", tx);
            TableScan ts = new TableScan(tx, "viewcat", layout);
            ts.Insert();
            ts.SetValue("viewname", vname);
            ts.SetValue("viewdef", vdef);
            ts.Close();
        }

        public string? getViewDef(String vname, Transaction tx)
        {
            string? result = null;
            Layout layout = tblMgr.getLayout("viewcat", tx);
            DbString vnameConstant = new DbString(vname);
            TableScan ts = new TableScan(tx, "viewcat", layout);
            while (ts.Next())
                //if (ts.CompareString("viewname", vnameConstant))
                if (ts.GetValue("viewname")  == vnameConstant)
                {
                    result = ts.GetString("viewdef");
                    break;
                }
            ts.Close();
            return result;
        }
    }
}
