using Microsoft.AspNetCore.Mvc;
using StackExchange.Profiling;

namespace SimpleDbNET.Api.Controllers
{
    [Route("home")]
    [ApiController]
    [Produces("application/json")]
    public class HomeController : Controller
    {
        [HttpGet]
        public ActionResult Index()
        {
            using (MiniProfiler.Current.Step("Example Step"))
            {
                using (MiniProfiler.Current.Step("Sub timing"))
                {
                    // Not trying to delay the page load here, only serve as an example
                }
                using (MiniProfiler.Current.Step("Sub timing 2"))
                {
                    // Not trying to delay the page load here, only serve as an example
                }
            }
            return Ok();
        }
    }
}
