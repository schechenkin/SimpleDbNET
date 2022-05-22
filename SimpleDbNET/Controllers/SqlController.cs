using Microsoft.AspNetCore.Mvc;
using SimpleDb;
using StackExchange.Profiling;

namespace SimpleDbNET.Api.Controllers
{
    [Route("sql")]
    [ApiController]
    [Produces("application/json")]
    public class SqlController : Controller
    {
        [HttpPost]
        public async Task<ActionResult> Run([FromBody] string sql, [FromServices] ISimpleDbServer db)
        {
            if(sql.StartsWith("select"))
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
