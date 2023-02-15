using Microsoft.AspNetCore.Mvc;

namespace SimpleDbNET.Api.Controllers
{
    [Route("values")]
    [ApiController]
    [Produces("application/json")]
    public class ValuesController : ControllerBase
    {
        [HttpGet]
        public ActionResult<IEnumerable<string>> Get()
        {
            return new string[] { "value1", "value2", "value3"  };
        }

        [HttpGet("sum/{a}/{b}")]
        public async Task<ActionResult<int>> Sum(int a, int b)
        {
            return Ok(a + b);
        }
    }
}
