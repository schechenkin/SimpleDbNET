using Serilog;
using SimpleDbNET.Api;

public class Program
{
    public static void Main(string[] args)
    {
        var webHost = CreateHostBuilder(args).Build();

        webHost.Run();
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureWebHostDefaults(webBuilder =>
            {               
                webBuilder
                    .UseStartup<Startup>();
            });
}