using System.Collections.Immutable;
using Microsoft.Extensions.Logging;

namespace SimpleDbNET.UnitTests.Fixtures;

public static class TestLoggerFactory
{
    static TestLoggerFactory()
    {
        Instance = LoggerFactory.Create(Builder => { });
    }
    
    public static ILoggerFactory Instance;
}
