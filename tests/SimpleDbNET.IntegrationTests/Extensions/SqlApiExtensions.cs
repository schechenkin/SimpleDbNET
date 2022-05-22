using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDbNET.IntegrationTests.Extensions
{
    internal static class SqlApiExtensions
    {
        public static Task<HttpResponseMessage> ExecuteSql(this HttpClient client, string query)
        {
            return client.PostAsync($"sql", query.ToJsonContent());
        }
    }
}
