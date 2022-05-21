using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace SimpleDbNET.IntegrationTests.Extensions
{
    public static class JsonExtensions
    {
        static JsonSerializerOptions settings;

        static JsonExtensions()
        {
            settings = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = true
            };
            settings.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
        }

        public static HttpContent ToJsonContent(this object payload)
        {
            return new StringContent(JsonSerializer.Serialize(payload, settings), Encoding.UTF8, "application/json");
        }

        public static string ToJson(this object payload)
        {
            return JsonSerializer.Serialize(payload, settings);
        }

        public static async Task<T> BodyAs<T>(this HttpResponseMessage response)
        {
            var content = await response.Content.ReadAsStringAsync();

            try
            {
                return JsonSerializer.Deserialize<T>(content, settings);
            }
            catch (Exception e)
            {
                throw new Exception($"Error while DeserializeObject string {content}");
            }
        }

        public static T StringAs<T>(this string input)
        {
            return JsonSerializer.Deserialize<T>(input, settings);
        }
    }
}
