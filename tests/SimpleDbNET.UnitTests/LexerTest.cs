using FluentAssertions;
using SimpleDB.QueryParser;
using Xunit;

namespace SimpleDbNET.UnitTests
{
    public class LexerTest
    {
        [Fact]
        public void When_input_string_contains_variable_assigment()
        {
			String s = "pep = 3";
			Lexer lex = new Lexer(s);
			String x; int y;

			lex.matchId().Should().BeTrue();
			x = lex.eatId();
			lex.eatDelim('=');
			y = lex.eatIntConstant();

			x.Should().Be("pep");
			y.Should().Be(3);
		}
    }
}
