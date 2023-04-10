using SimpleDb.Transactions;
using System.Diagnostics;
using SimpleDb.Buffers;
using SimpleDb.Types;
using SimpleDb.Abstractions;

namespace SimpleDb;

public interface ISimpleDbServer
{
    Task ExecuteUpdateSql(string sql);
    Task DropDb();
    Task<SelectResult> ExecuteSelectSql(string sql, int limit = 100);
    BufferManager.UsageStats GetBufferManagerUsage();
    ILogManager Log {get;}
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

    public List<string> Columns { get; set; } = new List<string>();
    public List<List<object>> Rows { get; set; } = new List<List<object>>();
    public long ElapsedMilliseconds { get; set; }

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

    internal void AddDateTimeColumn(string column, DateTime dt)
    {
        Rows.Last().Add(dt.ToString());
    }

    internal void AddNullColumn(string column)
    {
        Rows.Last().Add("null");
    }
}

internal class SimpleDbConext : ISimpleDbServer
{
    private Server db;

    public ILogManager Log => db.LogManager;

    public SimpleDbConext()
    {
        db = new Server("database");
    }

    public Task DropDb()
    {
        db.FileManager.CloseFiles();

        db = new Server("database", true);
        return Task.CompletedTask;
    }

    public Task ExecuteUpdateSql(string sql)
    {
        Transaction tx = db.NewTransaction();
        db.Planner.executeUpdate(sql, tx);
        tx.Commit();

        return Task.CompletedTask;
    }

    public Task<SelectResult> ExecuteSelectSql(string sql, int limit = 100)
    {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();

        Transaction tx = db.NewTransaction();
        var plan = db.Planner.createQueryPlan(sql, tx);
        var schema = plan.schema();
        var columns = schema.ColumnNames();
        var result = new SelectResult(columns);
        var scan = plan.open();
        int rowsCounter = 0;
        while (scan.Next())
        {
            result.AddRow();

            foreach (var column in columns)
            {
                if (scan.IsNull(column))
                {
                    result.AddNullColumn(column);
                }
                else
                {
                    var sqlType = schema.GetSqlType(column);
                    switch (sqlType)
                    {
                        case SqlType.INTEGER:
                            result.AddIntColumn(column, scan.GetInt(column));
                            break;
                        case SqlType.VARCHAR:
                            result.AddStringColumn(column, scan.GetString(column).GetString());
                            break;
                        case SqlType.DATETIME:
                            result.AddDateTimeColumn(column, scan.GetDateTime(column));
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                }
            }

            rowsCounter++;

            if (rowsCounter >= limit)
                break;
        }

        tx.Commit();

        stopwatch.Stop();

        result.ElapsedMilliseconds = stopwatch.ElapsedMilliseconds;

        return Task.FromResult(result);
    }

    public BufferManager.UsageStats GetBufferManagerUsage()
    {
        return db.BufferManager.GetUsageStats();
    }
}
