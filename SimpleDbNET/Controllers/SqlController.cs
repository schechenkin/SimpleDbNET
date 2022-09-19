using Microsoft.AspNetCore.Mvc;
using SimpleDb;

namespace SimpleDbNET.Api.Controllers
{
    [Route("sql")]
    [ApiController]
    [Produces("application/json")]
    public class SqlController : Controller
    {
        [HttpPost]
        public async Task<ActionResult> Run([FromBody] string sql, [FromServices] ISimpleDbServer db, [FromServices] ILogger<SqlController> logger)
        {
            logger.LogDebug(sql);

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
    }
}
