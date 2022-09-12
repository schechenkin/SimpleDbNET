using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Microsoft.Extensions.Primitives;
using SimpleDb.QueryParser;
//using Microsoft.Extensions.Primitives;
//using SimpleDB.QueryParser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDbNET.Benchmarks
{
    [SimpleJob(RuntimeMoniker.Net60)]
    [MemoryDiagnoser]
    public class StringTokenizerBenchmark
    {
        QueryTokenizer ownTokenizer;
        Microsoft.Extensions.Primitives.StringTokenizer microsoftTokenizer;
        string sql = "select columnName from T1 where b=3";
        char[] delimeters = new char[] { ' ', ',', '(', ')' };

        [Benchmark]
        public string OwnVersion()
        {
            ownTokenizer = new QueryTokenizer(sql);

            ownTokenizer.NextToken();

            var token = ownTokenizer.CurrentToken;

            ownTokenizer.NextToken();

            token = ownTokenizer.CurrentToken;

            ownTokenizer.NextToken();

            token = ownTokenizer.CurrentToken;

            ownTokenizer.NextToken();

            token = ownTokenizer.CurrentToken;

            ownTokenizer.NextToken();

            token = ownTokenizer.CurrentToken;

            ownTokenizer.NextToken();

            token = ownTokenizer.CurrentToken;

            ownTokenizer.NextToken();

            token = ownTokenizer.CurrentToken;

            ownTokenizer.NextToken();

            token = ownTokenizer.CurrentToken;

            return token;
        }

        [Benchmark]
        public StringSegment MicrosoftVersion()
        {
            StringSegment token = "";

            microsoftTokenizer = new Microsoft.Extensions.Primitives.StringTokenizer(sql, delimeters);

            var enumerator = microsoftTokenizer.GetEnumerator();

            while(enumerator.MoveNext())
            {
                token = enumerator.Current;
            }

            return token;
        }
    }
}
