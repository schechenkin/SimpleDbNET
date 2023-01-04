using Microsoft.AspNetCore.Mvc;
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

        [HttpPost("test")]
        public async Task<ActionResult> Run2([FromServices] ISimpleDbServer db, [FromServices] ILogger<SqlController> logger, [FromBody] string sql)
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
    }
}
