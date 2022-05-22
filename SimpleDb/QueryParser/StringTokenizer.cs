using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.QueryParser
{
    class StringTokenizer
    {
        private readonly string input;
        private List<string> tokens = new List<string>();
        private int currentTokenIndex = -1;

        public StringTokenizer(string input)
        {
            this.input = input;
            var tmp_tokens = input.Split(new char[] { ' ' }, StringSplitOptions.None).ToList();
            foreach(var tmp_token in tmp_tokens)
            {
                tokens.AddRange(AdditionalSplit(tmp_token));
            }
        }

        private IEnumerable<string> AdditionalSplit(string tmp_token)
        {
            List<string> res = new List<string>();
            int currentTokenStartsFrom = 0;

            for(int index = 0; index < tmp_token.Length; index++)
            {
                char currentSymbol = tmp_token[index];
                if(IsDelimeter(currentSymbol))
                {

                    string substr = tmp_token.Substring(currentTokenStartsFrom, index - currentTokenStartsFrom);
                    if(!string.IsNullOrEmpty(substr))
                        res.Add(substr);
                    res.Add(currentSymbol.ToString());
                    index++;
                    currentTokenStartsFrom = index;
                }
            }

            if(currentTokenStartsFrom < tmp_token.Length)
                res.Add(tmp_token.Substring(currentTokenStartsFrom, tmp_token.Length - currentTokenStartsFrom));

            return res;
        }

        private bool IsDelimeter(char currentSymbol)
        {
            return currentSymbol == ',' || currentSymbol == '=' || currentSymbol == '(' || currentSymbol == ')';
        }

        public void NextToken()
        {
            currentTokenIndex++;
        }

        public TokenType CurrentTokenType
        {
            get
            {
                if (CurrentToken.Length == 1 && IsDelimeter(CurrentToken[0]))
                    return TokenType.Delimiter;

                int res;
                if (int.TryParse(CurrentToken, out res))
                    return TokenType.Number;

                if (string.IsNullOrEmpty(CurrentToken))
                    return TokenType.Delimiter;

                return TokenType.Word;
            }
        }

        public string CurrentToken
        {
            get
            {
                if (currentTokenIndex < 0 || currentTokenIndex >= tokens.Count)
                    return string.Empty;

                return tokens[currentTokenIndex];
            }
        }
    }

    enum TokenType
    {
        Number,
        Word,
        Delimiter
    }
}
