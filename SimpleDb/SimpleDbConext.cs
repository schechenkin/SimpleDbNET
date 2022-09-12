using SimpleDB;
using SimpleDB.Tx;

namespace SimpleDb
{
    public interface ISimpleDbServer
    {
        Task ExecuteUpdateSql(string sql);
        Task DropDb();

        Task<SelectResult> ExecuteSelectSql(string sql);
    }

    public class SelectResult
    {
        public SelectResult(List<string> columns)
        {
            Columns = columns;
        }

        protected SelectResult()
        {

        }

        public List<string> Columns { get; set; }
        public List<List<object>> Rows { get; set; } = new List<List<object>>();

        internal void AddRow()
        {
            Rows.Add(new List<object>());
        }

        internal void AddIntColumn(string column, int v)
        {
            Rows.Last().Add(v);
        }

        internal void AddStringColumn(string column, string v)
        {
            Rows.Last().Add(v.Trim('\''));
        }
    }

    internal class SimpleDbConext : ISimpleDbServer
    {
        private Server db;

        public SimpleDbConext()
        {
            db = new Server("database");
        }

        public Task DropDb()
        {
            db.fileMgr().CloseFiles();

            db = new Server("database", true);
            return Task.CompletedTask;
        }

        public Task ExecuteUpdateSql(string sql)
        {
            Transaction tx = db.newTx();
            db.planner().executeUpdate(sql, tx);
            tx.Commit();

            return Task.CompletedTask;
        }

        public Task<SelectResult> ExecuteSelectSql(string sql)
        {
            Transaction tx = db.newTx();
            var plan = db.planner().createQueryPlan(sql, tx);
            var schema = plan.schema();
            var columns = schema.ColumnNames();
            var result = new SelectResult(columns);
            var scan = plan.open();
            while(scan.next())
            {
                result.AddRow();

                foreach(var column in columns)
                {
                    var sqlType = schema.GetSqlType(column);
                    switch(sqlType)
                    {
                        case SqlType.INTEGER:
                            result.AddIntColumn(column, scan.getInt(column));
                            break;
                        case SqlType.VARCHAR:
                            result.AddStringColumn(column, scan.getString(column));
                            break;
                    }
                }
            }

            return Task.FromResult(result);
        }
    }
}
