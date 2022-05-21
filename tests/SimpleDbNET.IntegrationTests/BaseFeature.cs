using Microsoft.Extensions.DependencyInjection;
using FluentAssertions;
using LightBDD.XUnit2;
using System.Net;

namespace SimpleDbNET.IntegrationTests
{
    public abstract class BaseFeature : FeatureFixture, IDisposable
    {
        protected HttpResponseMessage _response;

        protected HttpResponseMessage LastResponse => _response;

        public BaseFeature()
        {
        }

        protected T GetService<T>() => ServiceScopeFactory.GetService<T>();

        protected void EnsureSuccessStatusCode()
        {
            _response.EnsureSuccessStatusCode();
        }

        protected async Task SendRequest(Func<Task<HttpResponseMessage>> action)
        {
            _response = await action();
        }

        protected async Task WaitUntilCondition(Func<Task<bool>> conditionMet, int seconds = 5)
        {
            var timeoutExpired = false;
            var startTime = DateTime.Now;
            bool conditionHasMeted = false;
            var currentScope = OwnScope.Scope;
            while (!conditionHasMeted && !timeoutExpired)
            {
                Thread.Sleep(100);
                timeoutExpired = DateTime.Now - startTime > TimeSpan.FromSeconds(seconds);
                using (var serviceScope = ServiceScopeFactory.CreateScope())
                {
                    OwnScope.Scope = serviceScope;
                    conditionHasMeted = await conditionMet();
                }
            }
            OwnScope.Scope = currentScope;

            conditionHasMeted.Should().BeTrue();
        }

        protected async Task WaitUntil(Func<Task> conditionMet, int seconds = 10)
        {
            var timeoutExpired = false;
            bool conditionHasMeted = false;
            var startTime = DateTime.Now;
            var currentScope = OwnScope.Scope;
            while (!conditionHasMeted && !timeoutExpired)
            {
                Thread.Sleep(100);
                timeoutExpired = DateTime.Now - startTime > TimeSpan.FromSeconds(seconds);
                using (var serviceScope = ServiceScopeFactory.CreateScope())
                {
                    OwnScope.Scope = serviceScope;
                    try
                    {
                        await conditionMet();
                        conditionHasMeted = true;
                    }
                    catch
                    {

                    }
                }
            }
            OwnScope.Scope = currentScope;

            conditionHasMeted.Should().BeTrue();
        }

        public virtual void Dispose()
        {
            using (var serviceScope = TestServer.ClientFactory.Server.Services.GetService<IServiceScopeFactory>().CreateScope())
            {

            }

        }
        protected async Task Then_server_returns_bad_request_response()
        {
            _response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        }

        protected async Task Then_server_returns_not_found_response()
        {
            _response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        }

        protected async Task Then_server_returns_no_access_response()
        {
            _response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        }

        protected async Task Then_server_returns_OK_response()
        {
            _response.StatusCode.Should().Be(HttpStatusCode.OK);
        }
    }
}
