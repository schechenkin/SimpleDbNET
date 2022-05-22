using Microsoft.AspNetCore.Mvc;

namespace SimpleDbNET.Api.Views.Shared.Components.ComponentTest
{
    public class ComponentTestViewComponent : ViewComponent
    {
        public IViewComponentResult Invoke() => View();
    }
}
