using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.QueryParser
{
    class Lexer
    {
        private List<String> keywords;
        private StringTokenizer tok;

        /**
         * Creates a new lexical analyzer for SQL statement s.
         * @param s the SQL statement
         */
        public Lexer(String s)
        {
            initKeywords();
            tok = new StringTokenizer(s);
            //tok.ordinaryChar('.');   //disallow "." in identifiers
            //tok.wordChars('_', '_'); //allow "_" in identifiers
            //tok.lowerCaseMode(true); //ids and keywords are converted
            nextToken();
        }

        //Methods to check the status of the current token

        /**
         * Returns true if the current token is
         * the specified delimiter character.
         * @param d a character denoting the delimiter
         * @return true if the delimiter is the current token
         */
        public bool matchDelim(char d)
        {
            return tok.CurrentTokenType == TokenType.Delimiter && !string.IsNullOrEmpty(tok.CurrentToken) && tok.CurrentToken[0] == d;
        }

        /**
         * Returns true if the current token is an integer.
         * @return true if the current token is an integer
         */
        public bool matchIntConstant()
        {
            return tok.CurrentTokenType == TokenType.Number;
        }

        /**
         * Returns true if the current token is a string.
         * @return true if the current token is a string
         */
        public bool matchStringConstant()
        {
            return tok.CurrentTokenType == TokenType.Word;
        }

        /**
         * Returns true if the current token is the specified keyword.
         * @param w the keyword string
         * @return true if that keyword is the current token
         */
        public bool matchKeyword(String w)
        {
            return tok.CurrentToken.Equals(w) && tok.CurrentTokenType == TokenType.Word;
        }

        /**
         * Returns true if the current token is a legal identifier.
         * @return true if the current token is an identifier
         */
        public bool matchId()
        {
            return tok.CurrentTokenType == TokenType.Word && !keywords.Contains(tok.CurrentToken);
        }

        //Methods to "eat" the current token

        /**
         * Throws an exception if the current token is not the
         * specified delimiter. 
         * Otherwise, moves to the next token.
         * @param d a character denoting the delimiter
         */
        public void eatDelim(char d)
        {
            if (!matchDelim(d))
                throw new BadSyntaxException();
            nextToken();
        }

        /**
         * Throws an exception if the current token is not 
         * an integer. 
         * Otherwise, returns that integer and moves to the next token.
         * @return the integer value of the current token
         */
        public int eatIntConstant()
        {
            if (!matchIntConstant())
                throw new BadSyntaxException();
            int i = int.Parse(tok.CurrentToken);

            nextToken();
            return i;
        }

        /**
         * Throws an exception if the current token is not 
         * a string. 
         * Otherwise, returns that string and moves to the next token.
         * @return the string value of the current token
         */
        public String eatStringConstant()
        {
            if (!matchStringConstant())
                throw new BadSyntaxException();

            String s = tok.CurrentToken; //constants are not converted to lower case
            nextToken();
            return s;
        }

        /**
         * Throws an exception if the current token is not the
         * specified keyword. 
         * Otherwise, moves to the next token.
         * @param w the keyword string
         */
        public void eatKeyword(String w)
        {
            if (!matchKeyword(w))
                throw new BadSyntaxException();
            nextToken();
        }

        /**
         * Throws an exception if the current token is not 
         * an identifier. 
         * Otherwise, returns the identifier string 
         * and moves to the next token.
         * @return the string value of the current token
         */
        public String eatId()
        {
            if (!matchId())
                throw new BadSyntaxException();

            String s = tok.CurrentToken;
            nextToken();
            return s;
        }

        private void nextToken()
        {
            try
            {
                tok.NextToken();
            }
            catch (Exception)
            {
                throw new BadSyntaxException();
            }
        }

        private void initKeywords()
        {
            keywords = new()
            {
                "select",
                "from",
                "where",
                "and",
                "insert",
                "into",
                "values",
                "delete",
                "update",
                "set",
                "create",
                "table",
                "int",
                "varchar",
                "view",
                "as",
                "index",
                "on"
            };
        }
    }
}
