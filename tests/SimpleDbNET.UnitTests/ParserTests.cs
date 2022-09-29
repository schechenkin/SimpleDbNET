using FluentAssertions;
using SimpleDB.QueryParser;
using Xunit;

namespace SimpleDbNET.UnitTests
{
    public class ParserTests
    {
        [Fact]
        public void When_valid_select()
        {
            string sql = "select Name from People";
            Parser parser = new Parser(sql);
            QueryData queryData = parser.query();
            queryData.Should().NotBeNull();
        }

        [Fact]
        public void When_valid_update()
        {
            string sql = "update People set Age = 33 where Id = 1";
            Parser parser = new Parser(sql);
            object res = parser.updateCmd();
            ModifyData modifyData = res as ModifyData;
            modifyData.Should().NotBeNull();
        }

        [Fact]
        public void When_update_with_string_constant()
        {
            string sql = "update People set Name = 'Bob' where Id = 1";
            Parser parser = new Parser(sql);
            object res = parser.updateCmd();
            ModifyData modifyData = res as ModifyData;
            modifyData.Should().NotBeNull();
        }

        [Fact]
        public void When_select_contains_more_comparison()
        {
            string sql = "select Name from People where Age > 10";
            Parser parser = new Parser(sql);
            QueryData queryData = parser.query();
            queryData.Should().NotBeNull();
            queryData.pred().ToString().Should().Be("Age>10");
        }

        [Fact]
        public void When_valid_create_table()
        {
            string sql = "create table T (Id int not null, B varchar(10))";
            Parser parser = new Parser(sql);
            object res = parser.updateCmd();
            CreateTableData createTableData = res as CreateTableData;
            createTableData.Should().NotBeNull();
            var schema = createTableData.newSchema();
            schema.Should().NotBeNull();
            schema.ColumnNames().Should().BeEquivalentTo("Id", "B");
            schema.IsNullable("Id").Should().BeFalse();
            schema.IsNullable("B").Should().BeTrue();
        }

        [Fact]
        public void When_select_contains_is_null_comparison()
        {
            string sql = "select Name from People where Age is null";
            Parser parser = new Parser(sql);
            QueryData queryData = parser.query();
            queryData.Should().NotBeNull();
            queryData.pred().ToString().Should().Be("Age is null");
        }
    }
}
