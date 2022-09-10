using FluentAssertions;
using SimpleDB.QueryParser;
using Xunit;

namespace SimpleDbNET.UnitTests
{
    public class ParserTests
    {
        [Fact]
        public void When_valid_select_select()
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
    }
}
