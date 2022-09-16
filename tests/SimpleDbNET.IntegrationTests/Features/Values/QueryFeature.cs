using FluentAssertions;
using LightBDD.Framework;
using LightBDD.Framework.Scenarios;
using LightBDD.XUnit2;
using SimpleDb;
using SimpleDbNET.IntegrationTests.Extensions;
using SimpleDbNET.IntegrationTests.Features.Fixtures;
using Xunit;

namespace SimpleDbNET.IntegrationTests.Features.Values
{
    [FeatureDescription(@"")]
    public partial class QueryFeature : BaseFeature, IClassFixture<TestUsersFixture>, IClassFixture<TestDbFixture>
    {
        public QueryFeature(TestUsersFixture users, TestDbFixture testDb)
        {
            Users = users;
            Db = testDb;
        }

        public TestUsersFixture Users { get; }
        public TestDbFixture Db { get; }

        [Scenario]
        public async Task Insert_and_select_test()
        {
            await Runner
                .AddAsyncSteps(
                    Db.Given_empty_db
                )
                .AddAsyncStep("When user sends a request to create Students db with 5 rows", async _ =>
                {
                    _response = await Users.AnonimousClient.ExecuteSql("create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)");

                    string s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
                    String[] studvals = {  "(1, 'joe', 10, 2021)",
                                           "(2, 'amy', 20, 2020)",
                                           "(3, 'max', 10, 2022)",
                                           "(4, 'sue', 20, 2022)",
                                           "(5, 'bob', 30, 2020)"};
                    for (int i = 0; i < studvals.Length; i++)
                    {
                        _response = await Users.AnonimousClient.ExecuteSql(s + studvals[i]);
                        EnsureSuccessStatusCode();
                    }
                })
                .AddAsyncStep("When user sends request to get all students", async _ =>
                {
                    _response = await Users.AnonimousClient.ExecuteSql("select SId, SName from STUDENT where GradYear > 2021");
                    EnsureSuccessStatusCode();
                })
                .AddAsyncStep("Then response contains 2 rows", async _ =>
                {
                    var selectResult = await _response.BodyAs<SelectResult>();
                    selectResult.Rows.Count.Should().Be(2);
                })
                .RunAsync();
        }
    }
}
