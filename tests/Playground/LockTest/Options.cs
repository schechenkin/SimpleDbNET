using CommandLine;

namespace Playground.LockTest;

public class Options
{
    [Option(Default = 10000000, HelpText = "shared locks requests")]    
    public int SharedLocks { get; set; }
    
    //[Option(Default = 0, HelpText = "exclusive locks requests")] 
    //public int ExclusiveLocks { get; set; }
    
    //[Option(Default = 2, HelpText = "clients count")] 
    //public int Clients { get; set; }

    [Option(Default = 0, HelpText = "timeout between lock and unlock")] 
    public int ProduceTimeout {get;set;}

    [Option(Default = 20, HelpText = "timeout between lock and unlock")] 
    public int NProd {get;set;}
}
