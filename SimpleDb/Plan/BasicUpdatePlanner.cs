using SimpleDB.Metadata;
using SimpleDB.Query;
using SimpleDB.QueryParser;
using SimpleDB.Tx;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.Plan
{
    internal class BasicUpdatePlanner : UpdatePlanner
    {
        private MetadataMgr mdm;

        public BasicUpdatePlanner(MetadataMgr mdm)
        {
            this.mdm = mdm;
        }

        public int executeDelete(DeleteData data, Transaction tx)
        {
            Plan p = new TablePlan(tx, data.tableName(), mdm);
            p = new SelectPlan(p, data.pred());
            UpdateScan us = (UpdateScan)p.open();
            int count = 0;
            while (us.next())
            {
                us.delete();
                count++;
            }
            us.close();
            return count;
        }

        public int executeModify(ModifyData data, Transaction tx)
        {
            Plan p = new TablePlan(tx, data.tableName(), mdm);
            p = new SelectPlan(p, data.pred());
            UpdateScan us = (UpdateScan)p.open();
            int count = 0;
            while (us.next())
            {
                Constant val = data.newValue().evaluate(us);
                us.setVal(data.targetField(), val);
                count++;
            }
            us.close();
            return count;
        }

        public int executeInsert(InsertData data, Transaction tx)
        {
            Plan p = new TablePlan(tx, data.tableName(), mdm);
            UpdateScan us = (UpdateScan)p.open();
            us.insert();
            var iter = data.vals().GetEnumerator();
            foreach (String fldname in data.fields())
            {
                iter.MoveNext();
                Constant val = iter.Current;
                us.setVal(fldname, val);
            }
            us.close();
            return 1;
        }

        public int executeCreateTable(CreateTableData data, Transaction tx)
        {
            mdm.createTable(data.tableName(), data.newSchema(), tx);
            return 0;
        }

        public int executeCreateView(CreateViewData data, Transaction tx)
        {
            mdm.createView(data.viewName(), data.viewDef(), tx);
            return 0;
        }
        public int executeCreateIndex(CreateIndexData data, Transaction tx)
        {
            mdm.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
            return 0;
        }
    }

}
