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
        public int RowsCount { get; set; }
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
            var result = new SelectResult();
            Transaction tx = db.newTx();
            var plan = db.planner().createQueryPlan(sql, tx);
            var scan = plan.open();
            while(scan.next())
            {
                result.RowsCount++;
            }

            return Task.FromResult(result);
        }
    }
}
