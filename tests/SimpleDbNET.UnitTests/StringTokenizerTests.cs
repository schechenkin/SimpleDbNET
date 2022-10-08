using FluentAssertions;
using Microsoft.Extensions.Primitives;
using SimpleDb.QueryParser;
using Xunit;

namespace SimpleDbNET.UnitTests
{
    public class StringTokenizerTests
    {
        [Fact]
        public void When_valid_select_sql()
        {
            QueryTokenizer tokenizer = new QueryTokenizer("select columnName from T1 where b=3", new char[] { ' ', '\n' }, new char[] { ',', '=', '(', ')' });
            var enumerator = tokenizer.GetEnumerator();

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("select"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("columnName"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("from"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("T1"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("where"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("b"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Delimiter);
            enumerator.Current.Should().Be(new StringSegment("="));

            enumerator.MoveNext().Should().BeTrue();

            enumerator.CurrentTokenType.Should().Be(TokenType.Number);
            enumerator.Current.Should().Be(new StringSegment("3"));

            enumerator.MoveNext().Should().BeFalse();
        }

        [Fact]
        public void When_input_contains_string()
        {
            QueryTokenizer tokenizer = new QueryTokenizer("select columnName from T1 where b = 'some string' and a = 2", new char[] { ' ', '\n' }, new char[] { ',', '=', '(', ')' });
            var enumerator = tokenizer.GetEnumerator();

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("select"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("columnName"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("from"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("T1"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("where"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("b"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Delimiter);
            enumerator.Current.Should().Be(new StringSegment("="));

            enumerator.MoveNext().Should().BeTrue();

            enumerator.CurrentTokenType.Should().Be(TokenType.String);
            enumerator.Current.Should().Be(new StringSegment("'some string'"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("and"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("a"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Delimiter);
            enumerator.Current.Should().Be(new StringSegment("="));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Number);
            enumerator.Current.Should().Be(new StringSegment("2"));

            enumerator.MoveNext().Should().BeFalse();
        }

        [Fact]
        public void When_input_contains_dateTime()
        {
            QueryTokenizer tokenizer = new QueryTokenizer("select columnName from T1 where b = '2000-08-20 00:52:40.252224-05' and a = 2", new char[] { ' ', '\n' }, new char[] { ',', '=', '(', ')' });
            var enumerator = tokenizer.GetEnumerator();

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("select"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("columnName"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("from"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("T1"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("where"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("b"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Delimiter);
            enumerator.Current.Should().Be(new StringSegment("="));

            enumerator.MoveNext().Should().BeTrue();

            enumerator.CurrentTokenType.Should().Be(TokenType.DateTime);
            enumerator.Current.Should().Be(new StringSegment("'2000-08-20 00:52:40.252224-05'"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("and"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("a"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Delimiter);
            enumerator.Current.Should().Be(new StringSegment("="));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Number);
            enumerator.Current.Should().Be(new StringSegment("2"));

            enumerator.MoveNext().Should().BeFalse();
        }

        [Fact]
        public void When_input_contains_multiline_sql()
        {
            QueryTokenizer tokenizer = new QueryTokenizer(@"create table account (
    account_id int not null
)", new char[] { ' ', '\n', '\r' }, new char[] { ',', '=', '(', ')' });
            var enumerator = tokenizer.GetEnumerator();

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("create"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("table"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("account"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Delimiter);
            enumerator.Current.Should().Be(new StringSegment("("));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("account_id"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("int"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("not"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Word);
            enumerator.Current.Should().Be(new StringSegment("null"));

            enumerator.MoveNext();

            enumerator.CurrentTokenType.Should().Be(TokenType.Delimiter);
            enumerator.Current.Should().Be(new StringSegment(")"));

            enumerator.MoveNext().Should().BeFalse();

        }
    }
}
