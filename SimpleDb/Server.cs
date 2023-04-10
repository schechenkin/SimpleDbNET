using SimpleDb.Transactions.Concurrency;
using SimpleDb.File;
using SimpleDb.Metadata;
using SimpleDb.Plan;
using SimpleDb.Transactions;
using SimpleDb.Log;
using SimpleDb.Buffers;
using SimpleDb.Abstractions;

namespace SimpleDb;

public class Server
{
    public static int BLOCK_SIZE = 4096;
    public static int BUFFER_SIZE = 1000 * 25 * 10;
    public static string LOG_FILE = "simpledb.log";

    private FileManager fileManager;
    private LogManager logManager;
    private BufferManager bufferManager;
    private MetadataMgr metaDataManager;
    private Planner planner;
    private LockTable lockTable;

    /**
     * A constructor useful for debugging.
     * @param dirname the name of the database directory
     * @param blocksize the block size
     * @param buffsize the number of buffers
     */
    private Server(string dirname, int blocksize, int buffsize, bool recreate = false)
    {
        fileManager = new FileManager(dirname, blocksize, recreate, 262144);
        logManager = new LogManager(new FileManager(dirname, 1024*1024*16, recreate, 100), LOG_FILE);
        bufferManager = new BufferManager(fileManager, logManager, buffsize);
        lockTable = new LockTable();

        Transaction tx = NewTransaction();
        bool isnew = fileManager.IsNew();
        if (isnew)
            Console.WriteLine("creating new database");
        else
        {
            Console.WriteLine("recovering existing database");
            tx.Recover();
        }
        metaDataManager = new MetadataMgr(isnew, tx);

        tx.Commit();

        QueryPlanner qp = new BasicQueryPlanner(metaDataManager);
        UpdatePlanner up = new BasicUpdatePlanner(metaDataManager);
        //QueryPlanner qp = new HeuristicQueryPlanner(mdm);
        //UpdatePlanner up = new IndexUpdatePlanner(mdm);
        planner = new Planner(qp, up);
    }

    /**
     * A simpler constructor for most situations. Unlike the
     * 3-arg constructor, it also initializes the metadata tables.
     * @param dirname the name of the database directory
     */
    public Server(string dirname, bool recreate = false)
        : this(dirname, BLOCK_SIZE, BUFFER_SIZE, recreate)
    {

    }

    public Transaction NewTransaction()
    {
        return new Transaction(fileManager, logManager, bufferManager, lockTable);
    }


    // These methods aid in debugging
    public FileManager FileManager => fileManager;
    public BufferManager BufferManager => bufferManager;
    public Planner Planner => planner;
    public ILogManager LogManager => logManager;
}
