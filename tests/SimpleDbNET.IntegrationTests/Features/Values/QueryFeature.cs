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
        public async Task Index_test()
        {
            await Runner
                .AddAsyncSteps(
                    Db.Given_empty_db
                )
                .AddAsyncStep("Given sudents table with 100 records", async _ =>
                {
                    _response = await Users.AnonimousClient.ExecuteSql("create table Students(Id int, Name varchar(10))");

                     for (int i = 1; i <= 100; i++)
                    {
                        _response = await Users.AnonimousClient.ExecuteSql($"insert into Students(Id, Name) values ({i}, 'firstName{i}')");
                        EnsureSuccessStatusCode();
                    }
                })
                .AddAsyncStep("Given index on column Id", async _ =>
                {
                    _response = await Users.AnonimousClient.ExecuteSql("create index StudentIdIndex on Students (Id)");
                    EnsureSuccessStatusCode();
                })
                .AddAsyncStep("When sql select by Id = 42", async _ =>
                {
                    _response = await Users.AnonimousClient.ExecuteSql("select Id, Name from Students where Id = 42");
                    EnsureSuccessStatusCode();
                })
                .AddAsyncStep("Then response contains 1 row", async _ =>
                {
                    var selectResult = await _response.BodyAs<SelectResult>();
                    selectResult.Rows.Count.Should().Be(1);
                })
                .RunAsync();
        }

        [Scenario]
        public async Task Insert_and_select_test()
        {
            await Runner
                .AddAsyncSteps(
                    Db.Given_empty_db
                )
                .AddAsyncStep("When user sends a request to create Students db with 5 rows", async _ =>
                {
                    _response = await Users.AnonimousClient.ExecuteSql("create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int, BirthDay dateTime)");

                    string s = "insert into STUDENT(SId, SName, MajorId, GradYear, BirthDay) values ";
                    String[] studvals = {  "(1, 'joe', 10, 2021, '2000-08-20 00:52:40.252224-05')",
                                           "(2, 'amy', 20, 2020, '2001-08-20 00:52:40.252224-05')"};
                    for (int i = 0; i < studvals.Length; i++)
                    {
                        _response = await Users.AnonimousClient.ExecuteSql(s + studvals[i]);
                        EnsureSuccessStatusCode();
                    }

                    _response = await Users.AnonimousClient.ExecuteSql(s + "(3, 'max', 10, 2022, '2002-08-20 00:52:40.252224-05'), (4, 'sue', 20, 2022, '2003-08-20 00:52:40.252224-05'), (5, 'bob', 30, 2020, '2004-08-20 00:52:40.252224-05')");
                    EnsureSuccessStatusCode();
                })
                .AddAsyncStep("When user sends request to get all students", async _ =>
                {
                    _response = await Users.AnonimousClient.ExecuteSql("select SId, SName, BirthDay from STUDENT where GradYear > 2021");
                    EnsureSuccessStatusCode();
                })
                .AddAsyncStep("Then response contains 2 rows", async _ =>
                {
                    var selectResult = await _response.BodyAs<SelectResult>();
                    selectResult.Rows.Count.Should().Be(2);
                })
                .RunAsync();
        }

        [Scenario]
        public async Task Insert_and_select_test_with_nulls()
        {
            await Runner
                .AddAsyncSteps(
                    Db.Given_empty_db
                )
                .AddAsyncStep("When user sends a request to create Students db with 5 rows", async _ =>
                {
                    _response = await Users.AnonimousClient.ExecuteSql("create table STUDENT(SId int not null, SName varchar(10) not null, MajorId int not null, GradYear int not null, WorkExperience int)");

                    string s = "insert into STUDENT(SId, SName, MajorId, GradYear, WorkExperience) values ";
                    String[] studvals = {  "(1, 'joe', 10, 2021, 1)",
                                           "(2, 'amy', 20, 2020, null)",
                                           "(3, 'max', 10, 2022, 2)",
                                           "(4, 'sue', 20, 2022, null)",
                                           "(5, 'bob', 30, 2020, 2)"};
                    for (int i = 0; i < studvals.Length; i++)
                    {
                        _response = await Users.AnonimousClient.ExecuteSql(s + studvals[i]);
                        EnsureSuccessStatusCode();
                    }
                })
                .AddAsyncStep("When user sends request to get all students", async _ =>
                {
                    _response = await Users.AnonimousClient.ExecuteSql("select SId, SName from STUDENT where WorkExperience is null");
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
