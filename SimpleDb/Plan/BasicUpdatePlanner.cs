using SimpleDB.Indexes;
using SimpleDB.Metadata;
using SimpleDB.Query;
using SimpleDB.QueryParser;
using SimpleDB.Record;
using SimpleDB.Tx;

namespace SimpleDB.Plan
{
    /*internal class BasicUpdatePlanner : UpdatePlanner
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
            Plan p = new TablePlan(tx, data.tableName(), metadataManager);
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
            Plan p = new TablePlan(tx, data.tableName(), metadataManager);
            UpdateScan us = (UpdateScan)p.open();
            foreach(List<Constant> rowValues in data.vals())
            {
                us.insert();
                var iter = rowValues.GetEnumerator();
                foreach (string fldname in data.fields())
                {
                    iter.MoveNext();
                    Constant val = iter.Current;
                    us.setVal(fldname, val);
                }
            }
            us.close();
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
            string tableName = data.tableName();
            string columnName = data.fieldName();
            string indexName = data.indexName();

            Layout tableLayout = metadataManager.getLayout(tableName, tx);
            if (!tableLayout.schema().HasField(columnName))
                throw new Exception($"column {columnName} not exists in table {tableName}");

            var idxinfo = metadataManager.getIndexInfo(tableName, tx);
            if(idxinfo.ContainsKey(columnName))
                throw new Exception($"index on column {columnName} already exists");

            metadataManager.createIndex(indexName, tableName, columnName, tx);

            fillIndex(tableName, columnName, tableLayout, tx);

            return 0;
        }

        private void fillIndex(string tableName, string columnName, Layout tableLayout, Transaction tx)
        {
            var idxinfo = metadataManager.getIndexInfo(tableName, tx);
            var index = idxinfo[columnName].open();

            TableScan tableScan = new TableScan(tx, tableName, tableLayout);
            tableScan.beforeFirst();

            while (tableScan.next())
            {
                index.insert(tableScan.getVal(columnName), tableScan.getRid());
            }

            tableScan.close();
        }
    }*/

}
