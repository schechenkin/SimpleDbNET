using Microsoft.Extensions.DependencyInjection;
using SimpleDb;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDbNET.IntegrationTests.Features.Fixtures
{
    public class TestDbFixture
    {
        public async Task Given_empty_db()
        {
            await OwnScope.Scope.ServiceProvider.GetRequiredService<ISimpleDbServer>().DropDb();
        }
    }
}
