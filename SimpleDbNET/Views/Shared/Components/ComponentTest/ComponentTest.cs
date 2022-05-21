using Microsoft.AspNetCore.Mvc;

namespace SimpleDbNET.Views.Shared.Components.ComponentTest
{
    public class ComponentTestViewComponent : ViewComponent
    {
        public IViewComponentResult Invoke() => View();
    }
}
