using Microsoft.Extensions.Primitives;

namespace SimpleDb.QueryParser
{
    public readonly struct QueryTokenizer : IEnumerable<StringSegment>
    {
        private readonly StringSegment _value;
        private readonly char[] _separators;
        private readonly char[] _delimeters;

        public QueryTokenizer(string value, char[] separators, char[] delimeters)
        {
            _value = value;
            _separators = separators;
            _delimeters = delimeters;
        }

        public QueryTokenizer(StringSegment value, char[] separators, char[] delimeters)
        {
            _value = value;
            _separators = separators;
            _delimeters = delimeters;
        }
        public Enumerator GetEnumerator() => new Enumerator(in _value, _separators, _delimeters);

        IEnumerator<StringSegment> IEnumerable<StringSegment>.GetEnumerator() => GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

        internal void NextToken()
        {
            throw new NotImplementedException();
        }

        public struct Enumerator : IEnumerator<StringSegment>
        {
            private readonly StringSegment _value;
            private readonly char[] _separators;
            private readonly char[] _delimeters;
            private readonly char[] _allSeparators;
            private int _index;

            internal Enumerator(in StringSegment value, char[] separators, char[] delimeters)
            {
                _value = value;
                _separators = separators;
                _delimeters = delimeters;
                _allSeparators = _separators.Concat(_delimeters).ToArray();
                Current = default;
                _index = 0;
            }

            public Enumerator(ref QueryTokenizer tokenizer)
            {
                _value = tokenizer._value;
                _separators = tokenizer._separators;
                _delimeters = tokenizer._delimeters;
                _allSeparators = _separators.Concat(_delimeters).ToArray();
                Current = default(StringSegment);
                _index = 0;
            }

            public StringSegment Current { get; private set; }
            public TokenType CurrentTokenType
            {
                get
                {
                    if (Current.Length == 1 && IsDelimeter(Current[0]))
                        return TokenType.Delimiter;

                    int res;
                    if (int.TryParse(Current, out res))
                        return TokenType.Number;

                    if (Current.Length == 0 || Current == "")
                        return TokenType.Delimiter;

                    if (Current[0] == '\'')
                    {
                        DateTime dt;
                        var currentToken = Current;
                        if (DateTime.TryParse(currentToken.Subsegment(1, currentToken.Length - 2), out dt))
                            return TokenType.DateTime;
                        else
                            return TokenType.String;
                    }

                    return TokenType.Word;
                }
            }

            private bool IsDelimeter(char currentSymbol)
            {
                return _delimeters.Contains(currentSymbol);
            }

            private bool IsSpace(StringSegment current)
            {
                return current.Length == 1 && current[0] == ' ';
            }

            private bool IsSeparator(StringSegment current)
            {
                return current.Length == 1 && _separators.Contains(current[0]);
            }

            object System.Collections.IEnumerator.Current => Current;

            public void Dispose()
            {
            }

            public bool MoveNext()
            {
                if (!_value.HasValue || _index == -1 || _index >= _value.Length)
                {
                    Current = default(StringSegment);
                    return false;
                }

                int nextSeparatorIndex = _value.IndexOfAny(_allSeparators, _index);

                if (nextSeparatorIndex == -1)
                {
                    nextSeparatorIndex = _value.Length;
                }

                Current = _value.Subsegment(_index, Math.Max(1, nextSeparatorIndex - _index));

                if(Current.Length > 0 && Current[0] == '\'')
                {
                    nextSeparatorIndex = _value.IndexOf('\'', _index + 1);
                    Current = _value.Subsegment(_index, Math.Max(1, nextSeparatorIndex - _index + 1));
                    _index = nextSeparatorIndex;
                }

                if (_index == nextSeparatorIndex)
                    _index++;
                else
                    _index = nextSeparatorIndex;

                while((IsSpace(Current) || IsSeparator(Current)) && MoveNext())
                {

                }

                if (_index > _value.Length)
                {
                    Current = default(StringSegment);
                    return false;
                }

                return true;
            }

            public void Reset()
            {
                Current = default(StringSegment);
                _index = 0;
            }
        }
    }

    public enum TokenType
    {
        Number,
        Word,
        Delimiter,
        String,
        DateTime
    }
}
