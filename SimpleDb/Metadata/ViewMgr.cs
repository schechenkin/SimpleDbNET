﻿using SimpleDB.Record;
using SimpleDB.Tx;
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
                sch.addStringField("viewname", TableMgr.MAX_NAME);
                sch.addStringField("viewdef", MAX_VIEWDEF);
                tblMgr.createTable("viewcat", sch, tx);
            }
        }

        public void createView(String vname, String vdef, Transaction tx)
        {
            Layout layout = tblMgr.getLayout("viewcat", tx);
            TableScan ts = new TableScan(tx, "viewcat", layout);
            ts.insert();
            ts.setString("viewname", vname);
            ts.setString("viewdef", vdef);
            ts.close();
        }

        public String getViewDef(String vname, Transaction tx)
        {
            String result = null;
            Layout layout = tblMgr.getLayout("viewcat", tx);
            TableScan ts = new TableScan(tx, "viewcat", layout);
            while (ts.next())
                if (ts.getString("viewname").Equals(vname))
                {
                    result = ts.getString("viewdef");
                    break;
                }
            ts.close();
            return result;
        }
    }
}
