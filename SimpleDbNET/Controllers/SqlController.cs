﻿using Microsoft.AspNetCore.Mvc;
using SimpleDb;

namespace SimpleDbNET.Api.Controllers
{
    [Route("sql")]
    public class SqlController : Controller
    {
        [HttpPost]
        public async Task<ActionResult> Run([FromServices] ISimpleDbServer db, [FromServices] ILogger<SqlController> logger)
        {
            string sql;

            using (StreamReader stream = new StreamReader(HttpContext.Request.Body))
            {
                sql = await stream.ReadToEndAsync();
            }

            //logger.LogDebug(sql);

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
            Console.WriteLine("SelectFlight");
            return Run2(db, logger, "select flight_id, flight_no, scheduled_departure,  scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code, actual_departure, actual_arrival, update_ts from flight where flight_id = 278663");
        }

        [HttpGet("test/select/account")]
        public Task<ActionResult> SelectAccount([FromServices] ISimpleDbServer db, [FromServices] ILogger<SqlController> logger)
        {
            Console.WriteLine("SelectAccount");
            return Run2(db, logger, "select account_id, login, first_name, last_name from account where account_id = 37407");
        }
    }
}
