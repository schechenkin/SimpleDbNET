using CommandLine;

namespace Playground.LockTableTest;

public class Options
{
    [Option(Default = 1000, HelpText = "shared locks requests per reader")]    
    public int SharedLocks { get; set; }

    [Option(Default = 10000, HelpText = "exclusive locks requests per writer")]    
    public int ExclusiveLocks { get; set; }
    
    [Option(Default = 4, HelpText = "writers count")] 
    public int Writers {get;set;}

    [Option(Default = 10, HelpText = "readers count")] 
    public int Readers {get;set;}

    [Option(Default = 1, HelpText = "reader timeout")] 
    public int ReadersTimeout {get;set;}

    [Option(Default = 1, HelpText = "writer timeout")] 
    public int WritersTimeout {get;set;}
}
