using Microsoft.AspNetCore.Server.Kestrel.Core;
using Serilog;
using SimpleDb.Extensions;
using SimpleDbNET.Api.Tracking;
using StackExchange.Profiling.Storage;
using System.Text.Json.Serialization;

namespace SimpleDbNET.Api
{
    public class Startup
    {
        private readonly IConfiguration _configuration;
        private readonly bool _isDevelopment;

        public Startup(IConfiguration configuration, IWebHostEnvironment env)
        {
            _configuration = configuration;
            _isDevelopment = env.IsDevelopment();
        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddLogging(logging => {
                var logger = new LoggerConfiguration()
                            .ReadFrom.Configuration(_configuration)
                            .Enrich.FromLogContext()
                            .CreateLogger();
                        logging.ClearProviders();
                        logging.AddSerilog(logger);
            });
            
            services
                .AddControllers(config => { })
                .AddJsonOptions(options =>
                {
                    options.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter());
                    options.JsonSerializerOptions.PropertyNameCaseInsensitive = true;
                });

            services
                .AddEndpointsApiExplorer()
                .AddSwaggerGen()
                .AddHttpContextAccessor()
                .AddHealthChecks();

            services.AddMvc(options =>
            {
                // Because the samples have some MyAction and MyActionAsync duplicates
                // See: https://github.com/aspnet/AspNetCore/issues/8998
                options.SuppressAsyncSuffixInActionNames = false;
            });

            services.AddSimpleDb(Configuration, new BlocksReadWriteTracker());

            ConfigureMiniProfiler(services);

            services.Configure<KestrelServerOptions>(options =>
            {
                options.AllowSynchronousIO = true;
            });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseMiniProfiler()
                .UseRouting()
                .UseSwagger(options =>
                {
                    options.SerializeAsV2 = true;
                })
                .UseStaticFiles()
                .UseSwaggerUI(options =>
                {
                })
                .UseEndpoints(endpoints =>
                {
                    endpoints.MapAreaControllerRoute("areaRoute", "MySpace",
                       "MySpace/{controller=Home}/{action=Index}/{id?}");
                    endpoints.MapControllerRoute("default_route", "{controller=Home}/{action=Index}/{id?}");

                    endpoints.MapRazorPages();
                    endpoints.MapGet("/named-endpoint", async httpContext =>
                    {
                        var endpointName = httpContext.GetEndpoint().DisplayName;
                        await httpContext.Response.WriteAsync($"Content from an endpoint named {endpointName}");
                    }).WithDisplayName("Named Endpoint");

                    endpoints.MapGet("implicitly-named-endpoint", async httpContext =>
                    {
                        var endpointName = httpContext.GetEndpoint().DisplayName;
                        await httpContext.Response.WriteAsync($"Content from an endpoint named {endpointName}");
                    });

                    endpoints.MapHealthChecks("/health");
                });
        }

        private void ConfigureMiniProfiler(IServiceCollection services)
        {
            if (_configuration.GetValue<bool>("MiniProfiler:Enabled"))
            {
                // Note .AddMiniProfiler() returns a IMiniProfilerBuilder for easy intellisense
                services.AddMiniProfiler(options =>
                {
                    // All of this is optional. You can simply call .AddMiniProfiler() for all defaults

                    // (Optional) Path to use for profiler URLs, default is /mini-profiler-resources
                    options.RouteBasePath = "/profiler";

                    // (Optional) Control storage
                    // (default is 30 minutes in MemoryCacheStorage)
                    // Note: MiniProfiler will not work if a SizeLimit is set on MemoryCache!
                    //   See: https://github.com/MiniProfiler/dotnet/issues/501 for details
                    (options.Storage as MemoryCacheStorage).CacheDuration = TimeSpan.FromMinutes(60);

                    // (Optional) Control which SQL formatter to use, InlineFormatter is the default
                    options.SqlFormatter = new StackExchange.Profiling.SqlFormatters.InlineFormatter();

                    // (Optional) To control authorization, you can use the Func<HttpRequest, bool> options:
                    // (default is everyone can access profilers)
                    //options.ResultsAuthorize = request => MyGetUserFunction(request).CanSeeMiniProfiler;
                    //options.ResultsListAuthorize = request => MyGetUserFunction(request).CanSeeMiniProfiler;
                    // Or, there are async versions available:
                    //options.ResultsAuthorizeAsync = async request => (await MyGetUserFunctionAsync(request)).CanSeeMiniProfiler;
                    //options.ResultsAuthorizeListAsync = async request => (await MyGetUserFunctionAsync(request)).CanSeeMiniProfilerLists;

                    // (Optional)  To control which requests are profiled, use the Func<HttpRequest, bool> option:
                    // (default is everything should be profiled)
                    //options.ShouldProfile = request => MyShouldThisBeProfiledFunction(request);

                    // (Optional) Profiles are stored under a user ID, function to get it:
                    // (default is null, since above methods don't use it by default)
                    // options.UserIdProvider = request => MyGetUserIdFunction(request);

                    // (Optional) Swap out the entire profiler provider, if you want
                    // (default handles async and works fine for almost all applications)
                    //options.ProfilerProvider = new MyProfilerProvider();

                    // (Optional) You can disable "Connection Open()", "Connection Close()" (and async variant) tracking.
                    // (defaults to true, and connection opening/closing is tracked)
                    options.TrackConnectionOpenClose = true;

                    // (Optional) Use something other than the "light" color scheme.
                    // (defaults to "light")
                    options.ColorScheme = StackExchange.Profiling.ColorScheme.Auto;

                    // Optionally change the number of decimal places shown for millisecond timings.
                    // (defaults to 2)
                    //options.PopupDecimalPlaces = 1;

                    // The below are newer options, available in .NET Core 3.0 and above:

                    // (Optional) You can disable MVC filter profiling
                    // (defaults to true, and filters are profiled)
                    options.EnableMvcFilterProfiling = true;
                    // ...or only save filters that take over a certain millisecond duration (including their children)
                    // (defaults to null, and all filters are profiled)
                    // options.MvcFilterMinimumSaveMs = 1.0m;

                    // (Optional) You can disable MVC view profiling
                    // (defaults to true, and views are profiled)
                    options.EnableMvcViewProfiling = true;
                    // ...or only save views that take over a certain millisecond duration (including their children)
                    // (defaults to null, and all views are profiled)
                    // options.MvcViewMinimumSaveMs = 1.0m;

                    // (Optional) listen to any errors that occur within MiniProfiler itself
                    // options.OnInternalError = e => MyExceptionLogger(e);

                    // (Optional - not recommended) You can enable a heavy debug mode with stacks and tooltips when using memory storage
                    // It has a lot of overhead vs. normal profiling and should only be used with that in mind
                    // (defaults to false, debug/heavy mode is off)
                    //options.EnableDebugMode = true;

                    options.IgnoredPaths.Add("/lib");
                    options.IgnoredPaths.Add("/css");
                    options.IgnoredPaths.Add("/js");
                });
            }
        }

        private bool IsTest()
        {
            var env_name = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            return env_name == "Test" || env_name == "Tests" || env_name == "Tests_local";
        }
    }
}
