using Microsoft.AspNetCore.Mvc;

namespace SimpleDbNET.Api.Controllers
{
    [Route("values")]
    [ApiController]
    [Produces("application/json")]
    public class ValuesController : ControllerBase
    {
        private readonly ILogger<ValuesController> logger;

        public ValuesController(ILogger<ValuesController> logger)
        {
            this.logger = logger;
        }
        
        [HttpGet]
        public ActionResult<IEnumerable<string>> Get()
        {
            return new string[] { "value1", "value2", "value3"  };
        }

        [HttpGet("sum/{a}/{b}")]
        public async Task<ActionResult<int>> Sum(int a, int b)
        {
            logger.LogInformation("calculate sum {a} + {b}", a, b);
            return Ok(a + b);
        }
    }
}
