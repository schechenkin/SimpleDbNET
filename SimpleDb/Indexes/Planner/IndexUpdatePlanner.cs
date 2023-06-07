using SimpleDb.Metadata;
using SimpleDb.Plan;
using SimpleDb.Query;
using SimpleDb.QueryParser;
using SimpleDb.Record;
using SimpleDb.Transactions;
using SimpleDb.Types;
using SimpleDB.Metadata;

namespace SimpleDb.Indexes.Planner
{
    internal class IndexUpdatePlanner : UpdatePlanner
    {
        private MetadataMgr mdm;

        public IndexUpdatePlanner(MetadataMgr mdm)
        {
            this.mdm = mdm;
        }

        public int executeDelete(DeleteData data, Transaction tx)
        {
            SimpleDb.Plan.Plan p = new TablePlan(tx, data.tableName(), mdm);
            p = new SelectPlan(p, data.pred());
            var indexes = mdm.getIndexInfo(data.tableName(), tx);
            UpdateScan us = (UpdateScan)p.open();
            int count = 0;
            while (us.Next())
            {
                RID rid = us.GetRid();
                foreach (var fldName in indexes.Keys)
                {
                    var val = us.GetValue(fldName);
                    var idx = indexes[fldName].open();
                    idx.delete(val, rid);
                    idx.close();
                }
                
                us.Delete();
                count++;
            }
            us.Close();
            return count;
        }

        public int executeModify(ModifyData data, Transaction tx)
        {
            string tableName = data.tableName();
            string fldName = data.targetField();
            SimpleDb.Plan.Plan p = new TablePlan(tx, tableName, mdm);
            p = new SelectPlan(p, data.pred());
            var indexes = mdm.getIndexInfo(tableName, tx);

            SimpleDb.Index? idx = null;
            if (indexes.ContainsKey(fldName))
            {
                idx = indexes[fldName].open();
            }

             UpdateScan us = (UpdateScan)p.open();
            int count = 0;
            while (us.Next())
            {
                // first, update the record
                Constant newVal = data.newValue().evaluate(us);
                Constant oldVal = us.GetValue(fldName);
                us.SetValue(data.targetField(), newVal);

                // then update the appropriate index, if it exists
                if(idx != null)
                {
                    RID rid = us.GetRid();
                    idx.delete(oldVal, rid);
                    idx.insert(newVal, rid);
                }
                count++;
            }
            if (idx != null)
                idx.close();
            us.Close();
            return count;
        }

        public int executeInsert(InsertData data, Transaction tx)
        {
            SimpleDb.Plan.Plan p = new TablePlan(tx, data.tableName(), mdm);
            UpdateScan updateScan = (UpdateScan)p.open();

            var indexes = mdm.getIndexInfo(data.tableName(), tx);

            foreach (List<Constant> rowValues in data.vals())
            {
                updateScan.Insert();
                var iter = rowValues.GetEnumerator();
                foreach (string fldname in data.fields())
                {
                    iter.MoveNext();
                    Constant val = iter.Current;
                    updateScan.SetValue(fldname, val);

                    RID rid = updateScan.GetRid();
                    if(indexes.ContainsKey(fldname))
                    {
                        IndexInfo ii = indexes[fldname];

                        var idx = ii.open();
                        idx.insert(val, rid);
                        idx.close();
                    }
                }
            }

            updateScan.Close();
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
            string tableName = data.tableName();
            string columnName = data.fieldName();
            string indexName = data.indexName();

            Layout tableLayout = mdm.getLayout(tableName, tx);
            if (!tableLayout.schema().HasField(columnName))
                throw new Exception($"column {columnName} not exists in table {tableName}");

            var idxinfo = mdm.getIndexInfo(tableName, tx);
            if (idxinfo.ContainsKey(columnName))
                throw new Exception($"index on column {columnName} already exists");

            mdm.createIndex(indexName, tableName, columnName, tx);

            fillIndex(tableName, columnName, tableLayout, tx);

            return 0;
        }

        private void fillIndex(string tableName, string columnName, Layout tableLayout, Transaction tx)
        {
            var idxinfo = mdm.getIndexInfo(tableName, tx);
            var index = idxinfo[columnName].open();

            TableScan tableScan = new TableScan(tx, tableName, tableLayout);
            tableScan.BeforeFirst();

            while (tableScan.Next())
            {
                index.insert(tableScan.GetValue(columnName), tableScan.GetRid());
            }

            tableScan.Close();
        }
    }
}
