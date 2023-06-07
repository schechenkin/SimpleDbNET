using System.Net.Http;
using System.Threading.Tasks;

namespace SimpleDbNET.IntegrationTests.Extensions
{
    internal static class ValuesApiExtensions
    {
        public static Task<HttpResponseMessage> GetSum(this HttpClient client, int a, int b)
        {
            return client.GetAsync($"values/sum/{a}/{b}");
        }
    }
}
