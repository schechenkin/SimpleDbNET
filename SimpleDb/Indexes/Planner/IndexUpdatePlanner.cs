using SimpleDB.Indexes;
using SimpleDB.Metadata;
using SimpleDB.Plan;
using SimpleDB.Query;
using SimpleDB.QueryParser;
using SimpleDB.Record;
using SimpleDB.Tx;

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
            Plan p = new TablePlan(tx, data.tableName(), mdm);
            p = new SelectPlan(p, data.pred());
            var indexes = mdm.getIndexInfo(data.tableName(), tx);
            UpdateScan us = (UpdateScan)p.open();
            int count = 0;
            while (us.next())
            {
                RID rid = us.getRid();
                foreach (var fldName in indexes.Keys)
                {
                    var val = us.getVal(fldName);
                    var idx = indexes[fldName].open();
                    idx.delete(val, rid);
                    idx.close();
                }
                
                us.delete();
                count++;
            }
            us.close();
            return count;
        }

        public int executeModify(ModifyData data, Transaction tx)
        {
            string tableName = data.tableName();
            string fldName = data.targetField();
            Plan p = new TablePlan(tx, tableName, mdm);
            p = new SelectPlan(p, data.pred());
            var indexes = mdm.getIndexInfo(tableName, tx);

            SimpleDB.Indexes.Index? idx = null;
            if (indexes.ContainsKey(fldName))
            {
                idx = indexes[fldName].open();
            }

             UpdateScan us = (UpdateScan)p.open();
            int count = 0;
            while (us.next())
            {
                // first, update the record
                Constant newVal = data.newValue().evaluate(us);
                Constant oldVal = us.getVal(fldName);
                us.setVal(data.targetField(), newVal);

                // then update the appropriate index, if it exists
                if(idx != null)
                {
                    RID rid = us.getRid();
                    idx.delete(oldVal, rid);
                    idx.insert(newVal, rid);
                }
                count++;
            }
            if (idx != null)
                idx.close();
            us.close();
            return count;
        }

        public int executeInsert(InsertData data, Transaction tx)
        {
            Plan p = new TablePlan(tx, data.tableName(), mdm);
            UpdateScan updateScan = (UpdateScan)p.open();

            var indexes = mdm.getIndexInfo(data.tableName(), tx);

            foreach (List<Constant> rowValues in data.vals())
            {
                updateScan.insert();
                var iter = rowValues.GetEnumerator();
                foreach (string fldname in data.fields())
                {
                    iter.MoveNext();
                    Constant val = iter.Current;
                    updateScan.setVal(fldname, val);

                    RID rid = updateScan.getRid();
                    if(indexes.ContainsKey(fldname))
                    {
                        IndexInfo ii = indexes[fldname];

                        var idx = ii.open();
                        idx.insert(val, rid);
                        idx.close();
                    }
                }
            }

            updateScan.close();
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
            tableScan.beforeFirst();

            while (tableScan.next())
            {
                index.insert(tableScan.getVal(columnName), tableScan.getRid());
            }

            tableScan.close();
        }
    }
}
