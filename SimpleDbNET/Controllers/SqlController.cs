using Microsoft.AspNetCore.Mvc;
using SimpleDb;
using SimpleDb.Abstractions;
using SimpleDb.Checkpoint;
using SimpleDb.Transactions.Recovery;

namespace SimpleDbNET.Api.Controllers;

[Route("sql")]
public class SqlController : Controller
{
    [HttpPost]
    public async Task<ActionResult> Run([FromServices] ISimpleDbServer db, [FromServices] ActiveRequestsCounter activeRequestsCounter, [FromServices] ICheckpoint checkpoint, [FromServices] ILogger<SqlController> logger)
    {
        string sql;

        using (StreamReader stream = new StreamReader(HttpContext.Request.Body))
        {
            sql = await stream.ReadToEndAsync();
        }

        try
        {
            checkpoint.WaitCheckpointIsComplete();
            
            activeRequestsCounter.Increment();

            if (sql.StartsWith("select"))
            {
                return Ok(await db.ExecuteSelectSql(sql));
            }
            else
            {
                await db.ExecuteUpdateSql(sql);
                return Ok();
            }
        }
        catch (Exception ex)
        {
            logger.LogError("error execute sql: " + sql, ex.Message);
            throw;
        }
        finally
        {
            activeRequestsCounter.Decrement();
        }
    }

    [HttpPost("checkpoint")]
    public ActionResult Checkpoint([FromServices] ISimpleDbServer db, [FromServices] ICheckpoint checkpoint, [FromServices] ILogger<SqlController> logger)
    {
        checkpoint.WaitCheckpointIsComplete();
        checkpoint.Execute();
        return Ok();
    }

    [HttpGet("test")]
    public async Task<ActionResult> Run2([FromServices] ISimpleDbServer db, [FromServices] ILogger<SqlController> logger, [FromQuery] string sql)
    {
        try
        {
            if (sql.StartsWith("select"))
            {
                return Ok(await db.ExecuteSelectSql(sql));
            }
            else
            {
                await db.ExecuteUpdateSql(sql);
                return Ok();
            }
        }
        catch (Exception ex)
        {
            logger.LogError("error execute sql: " + sql, ex.Message);
            throw;
        }
    }

    [HttpGet("test/select/flight")]
    public Task<ActionResult> SelectFlight([FromServices] ISimpleDbServer db, [FromServices] ILogger<SqlController> logger)
    {
        //Console.WriteLine("SelectFlight");
        return Run2(db, logger, "select flight_id, flight_no, scheduled_departure,  scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code, actual_departure, actual_arrival, update_ts from flight where flight_id = 278663");
    }

    [HttpGet("test/select/account")]
    public Task<ActionResult> SelectAccount([FromServices] ISimpleDbServer db, [FromServices] ILogger<SqlController> logger)
    {
        //Console.WriteLine("SelectAccount");
        return Run2(db, logger, "select account_id, login, first_name, last_name from account where account_id = 37407");
    }

    [HttpGet("buffermanager")]
    public async Task<ActionResult> GetBufferManagerStats([FromServices] ISimpleDbServer db)
    {
        return Ok(db.GetBufferManagerUsage());
    }

    [HttpGet("log")]
    public async Task<ActionResult> GetLog([FromServices] ISimpleDbServer db)
    {
        List<string> logRecords = new();
        var iter = db.Log.GetIterator();
        while (iter.HasNext())
        {
            byte[] bytes = iter.Next();
            ILogRecord rec = LogRecordFactory.CreateLogRecord(bytes);
            if(rec != null)
            {
                logRecords.Add(rec.ToString());
                Console.WriteLine(rec.ToString());
            }
        }


        return Ok(logRecords);
    }

    [HttpGet("logreversed")]
    public async Task<ActionResult> GetLogReversed([FromServices] ISimpleDbServer db)
    {
        List<string> logRecords = new();
        var iter = db.Log.GetReverseIterator();
        while (iter.HasNext())
        {
            byte[] bytes = iter.Next();
            ILogRecord rec = LogRecordFactory.CreateLogRecord(bytes);
            if(rec != null)
            {
                logRecords.Add(rec.ToString());
                Console.WriteLine(rec.ToString());
            }
        }


        return Ok(logRecords);
    }
}
