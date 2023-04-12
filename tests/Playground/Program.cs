using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using Playground.LockTest;

public class Example
{
        public static void Main(string[] args)
        {
            CommandLine.Parser.Default.ParseArguments<Options>(args)
                .WithParsed(Run)
                .WithNotParsed(HandleParseError);
        }

        static void Run(Options opts)
        {
            TestRunner test = new TestRunner(opts);

            test.Run();
        }
        static void HandleParseError(IEnumerable<Error> errs)
        {
            foreach(var err in errs)
            {
                Console.WriteLine(err.ToString());
            }
        }
}