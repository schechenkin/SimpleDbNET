using AspNetCore.Testing.Authentication.ClaimInjector;

namespace SimpleDbNET.IntegrationTests.Features.Fixtures
{
    public class TestUsersFixture : IDisposable
    {
        public HttpClient AnonimousClient;
        private ClaimInjectorWebApplicationFactory<Startup> _factory;

        public TestUsersFixture()
        {
            _factory = TestServer.ClientFactory;

            _factory.RoleConfig.Reset();

            AnonimousClient = BuildAnonimousClient();
        }

        public void Dispose()
        {

        }

        protected HttpClient BuildClient(string account)
        {
            _factory.RoleConfig.Reset();
            _factory.RoleConfig.Name = account;

            return _factory.CreateDefaultClient(new HttpLoggingHandler(new HttpClientHandler()));
        }

        protected HttpClient BuildAnonimousClient()
        {
            _factory.RoleConfig.Reset();
            _factory.RoleConfig.AnonymousRequest = true;

            return _factory.CreateDefaultClient(new HttpLoggingHandler(new HttpClientHandler()));
        }
    }
}
