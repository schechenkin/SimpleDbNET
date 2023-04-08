using SimpleDb.Metadata;
using SimpleDb.Query;
using SimpleDb.QueryParser;
using SimpleDb.Record;
using SimpleDb.Transactions;
using SimpleDb.Types;

namespace SimpleDb.Plan
{
    internal class BasicUpdatePlanner : UpdatePlanner
    {
        private MetadataMgr metadataManager;

        public BasicUpdatePlanner(MetadataMgr mdm)
        {
            this.metadataManager = mdm;
        }

        public int executeDelete(DeleteData data, Transaction tx)
        {
            Plan p = new TablePlan(tx, data.tableName(), metadataManager);
            p = new SelectPlan(p, data.pred());
            UpdateScan us = (UpdateScan)p.open();
            int count = 0;
            while (us.Next())
            {
                us.Delete();
                count++;
            }
            us.Close();
            return count;
        }

        public int executeModify(ModifyData data, Transaction tx)
        {
            Plan p = new TablePlan(tx, data.tableName(), metadataManager);
            p = new SelectPlan(p, data.pred());
            UpdateScan us = (UpdateScan)p.open();
            int count = 0;
            while (us.Next())
            {
                Constant val = data.newValue().evaluate(us);
                us.SetValue(data.targetField(), val);
                count++;
            }
            us.Close();
            return count;
        }

        public int executeInsert(InsertData data, Transaction tx)
        {
            Plan p = new TablePlan(tx, data.tableName(), metadataManager);
            UpdateScan us = (UpdateScan)p.open();
            foreach(List<Constant> rowValues in data.vals())
            {
                us.Insert();
                var iter = rowValues.GetEnumerator();
                foreach (string fldname in data.fields())
                {
                    iter.MoveNext();
                    Constant val = iter.Current;
                    us.SetValue(fldname, val);
                }
            }
            us.Close();
            return 1;
        }

        public int executeCreateTable(CreateTableData data, Transaction tx)
        {
            metadataManager.createTable(data.tableName(), data.newSchema(), tx);
            return 0;
        }

        public int executeCreateView(CreateViewData data, Transaction tx)
        {
            metadataManager.createView(data.viewName(), data.viewDef(), tx);
            return 0;
        }

        public int executeCreateIndex(CreateIndexData data, Transaction tx)
        {
            throw new NotImplementedException();
        }
    }

}
