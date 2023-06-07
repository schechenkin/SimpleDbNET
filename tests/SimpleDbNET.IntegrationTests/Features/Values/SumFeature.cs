using FluentAssertions;
using LightBDD.Framework;
using LightBDD.Framework.Scenarios;
using LightBDD.XUnit2;
using SimpleDbNET.IntegrationTests;
using SimpleDbNET.IntegrationTests.Extensions;
using SimpleDbNET.IntegrationTests.Features.Fixtures;
using Xunit;

namespace SimpleDbNET.IntegrationTests.Features.Values
{
    [FeatureDescription(@"")]
    [Trait("Category", "Values")]
    public partial class Sum_feature : BaseFeature, IClassFixture<TestUsersFixture>
    {
        public Sum_feature(TestUsersFixture users)
        {
            Users = users;
        }

        public TestUsersFixture Users { get; }

        [Scenario]
        public async Task Sum_two_values()
        {
            await Runner.RunScenarioAsync(
                _ => When_user_send_request_to_get_sum_A_and_B(3, 5),
                _ => Then_response_contains_sum_SUM(8));
        }

        private async Task Then_response_contains_sum_SUM(int sum)
        {
            _response.EnsureSuccessStatusCode();
            var sum_response = await _response.BodyAs<int>();
            sum_response.Should().Be(sum);
        }

        private async Task When_user_send_request_to_get_sum_A_and_B(int a, int b)
        {
            _response = await Users.AnonimousClient.GetSum(a, b);
        }
    }
}
