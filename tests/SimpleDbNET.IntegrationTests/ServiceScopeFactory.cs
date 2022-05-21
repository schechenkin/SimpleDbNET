using Microsoft.Extensions.DependencyInjection;
using System;

namespace SimpleDbNET.IntegrationTests
{
    public static class ServiceScopeFactory
    {
        private static Func<IServiceScope> _scopeCreator;

        public static void Init(Func<IServiceScope> scopeCreator)
        {
            _scopeCreator = scopeCreator;
        }

        public static T GetService<T>()
        {
            return OwnScope.Scope.ServiceProvider.GetService<T>();
        }

        public static IServiceScope CreateScope()
        {
            return _scopeCreator();
        }

        public static IServiceScope GetCurrentScope()
        {
            return OwnScope.Scope;
        }

        public static void SetCurrentScope(IServiceScope scope)
        {
            OwnScope.Scope = scope;
        }
    }
}
