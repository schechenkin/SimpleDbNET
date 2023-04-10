using SimpleDb.File;
using SimpleDb.Transactions.Concurrency;
using System.Collections.Concurrent;

namespace Playground.LockTest;

public interface ILockTable
{
    void WaitExclusiveLock(BlockId blockId);
    void WaitSharedLock(BlockId blockId);
    void UnLock(BlockId blockId);
}

public class MyLockTable : ILockTable
{
    private Dictionary<BlockId, int> locks_ = new Dictionary<BlockId, int>();

    private static TimeSpan MAX_WAIT_TIME = TimeSpan.FromSeconds(10);

    public void WaitExclusiveLock(BlockId blockId)
    {
        lock (locks_)
        {
            DateTime timestamp = DateTime.Now;
            while (hasOtherSLocks(blockId) && !WaitingTooLong(timestamp))
                Monitor.Wait(locks_);
            if (hasOtherSLocks(blockId))
                throw new LockAbortException();

            if (!locks_.ContainsKey(blockId))
                locks_.Add(blockId, -1);
            else
                locks_[blockId] = -1;
        }
    }

    public void WaitSharedLock(BlockId blockId)
    {
        lock (locks_)
        {
            DateTime startWaitingTime = DateTime.Now;
            while (hasXlock(blockId) && !WaitingTooLong(startWaitingTime))
                Monitor.Wait(locks_);
            if (hasXlock(blockId))
                throw new LockAbortException();
            int lockValue = getLockVal(blockId);  // will not be negative

            if (!locks_.ContainsKey(blockId))
                locks_.Add(blockId, lockValue + 1);
            else
                locks_[blockId] = lockValue + 1;
        }
    }

    public void UnLock(BlockId blockId)
    {
        lock (locks_)
        {
            int lockValue = getLockVal(blockId);
            if (lockValue > 1)
                locks_[blockId] = lockValue - 1;
            else
            {
                locks_.Remove(blockId);
                Monitor.PulseAll(locks_);
            }
        }
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

public interface IQueue
{
    void Lock();
    void Wait();
}