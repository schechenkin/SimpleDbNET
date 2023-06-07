using LightBDD.Framework;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleDbNET.IntegrationTests
{
    public class HttpLoggingHandler : DelegatingHandler
    {
        public HttpLoggingHandler(HttpMessageHandler innerHandler)
            : base(innerHandler)
        {
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            StringBuilder builder = new StringBuilder();

            builder.Append($"\nRequest {request.Method} {request.RequestUri}");

            if (request.Content != null)
            {
                string requestBody = await request.Content.ReadAsStringAsync();
                builder.Append($" {requestBody}");
            }

            HttpResponseMessage response = await base.SendAsync(request, cancellationToken);

            builder.Append($"\nResponse {(int)response.StatusCode} ({response.StatusCode})");
            if (response.Content != null)
            {
                string responseBody = await response.Content.ReadAsStringAsync();
                if (responseBody.Length > 10000)
                {
                    builder.Append($" {responseBody.Substring(0, 100) + " ... (Тело ответа сокращено)"}");
                }
                else
                {
                    builder.Append($" {responseBody}");
                }
            }

            StepExecution.Current.Comment(builder.ToString());
            return response;
        }
    }
}
