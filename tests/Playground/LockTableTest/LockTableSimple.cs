using SimpleDb.File;
using SimpleDb.Transactions.Concurrency;

namespace Playground.LockTableTest;

public class LockTableSimple2: ILockTable
{
    private Dictionary<BlockId, int> locks_ = new Dictionary<BlockId, int>();

    private static TimeSpan MAX_WAIT_TIME = TimeSpan.FromSeconds(10);

    private object locker = new object();

    public void WaitExclusiveLock(in BlockId blockId)
    {
        Monitor.Enter(locker);
        
        /*lock (locker)
        {
            DateTime timestamp = DateTime.Now;
            while (hasOtherSLocks(blockId) && !WaitingTooLong(timestamp))
                Thread.SpinWait(1);

            if (hasOtherSLocks(blockId))
                throw new LockAbortException();

            if (!locks_.ContainsKey(blockId))
                locks_.Add(blockId, -1);
            else
                locks_[blockId] = -1;
        }*/
    }

    public void WaitSharedLock(in BlockId blockId)
    {

    }

    public void UnLock(in BlockId blockId)
    {
        Monitor.Exit(locker);
    }

    private bool hasXlock(in BlockId blockId)
    {
        return getLockVal(blockId) < 0;
    }

    private bool hasOtherSLocks(in BlockId blockId)
    {
        return getLockVal(blockId) > 1;
    }

    private bool WaitingTooLong(DateTime starttime)
    {
        return DateTime.Now - starttime > MAX_WAIT_TIME;
    }

    private int getLockVal(in BlockId blk)
    {
        int res = 0;
        if (locks_.TryGetValue(blk, out res))
        {
            return res;
        }
        return 0;
    }
}

