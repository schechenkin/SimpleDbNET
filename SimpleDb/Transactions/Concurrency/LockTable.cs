using System.Collections.Concurrent;
using SimpleDb.File;
namespace SimpleDb.Transactions.Concurrency;

public class LockTable
{
    private ConcurrentDictionary<BlockId, int> locks_ = new ConcurrentDictionary<BlockId, int>();

    private static TimeSpan MAX_WAIT_TIME = TimeSpan.FromSeconds(10);

    public void WaitExclusiveLock(in BlockId blockId)
    {
        /*lock (locks_)
        {
            DateTime timestamp = DateTime.Now;
            while (hasOtherSLocks(blockId) && !WaitingTooLong(timestamp))
                Monitor.Wait(locks_, MAX_WAIT_TIME);

            if (hasOtherSLocks(blockId))
                throw new LockAbortException();

            if (!locks_.ContainsKey(blockId))
                locks_.Add(blockId, -1);
            else
                locks_[blockId] = -1;
        }*/

        throw new NotImplementedException();
    }

    public void WaitSharedLock(in BlockId blockId)
    {
        /*lock (locks_)
        {
            DateTime startWaitingTime = DateTime.Now;
            while (hasXlock(blockId) && !WaitingTooLong(startWaitingTime))
                Monitor.Wait(locks_, MAX_WAIT_TIME);
            if (hasXlock(blockId))
                throw new LockAbortException();
            int lockValue = getLockVal(blockId);  // will not be negative

            if(!locks_.TryAdd(blockId, lockValue + 1))
            {
                locks_[blockId] = lockValue + 1;
            }
        }*/

        bool success = false;
        while (!success)
        {
            int lockValue = getLockVal(blockId);
            if (locks_.TryAdd(blockId, lockValue + 1))
            {
                //ok
                success = true;
            }
            else
            {
                //someone added already
                if (locks_.TryUpdate(blockId, lockValue + 1, lockValue))
                {
                    //ok
                    success = true;
                }
                else
                {
                    //someone changed lockvalue
                }
            }
        }
    }

    public void UnLock(in BlockId blockId)
    {
        /*lock (locks_)
        {
            int lockValue = getLockVal(blockId);
            if (lockValue > 1)
                locks_[blockId] = lockValue - 1;
            else
            {
                locks_.Remove(blockId);
                Monitor.PulseAll(locks_);
            }
        }*/

        bool success = false;
        while (!success)
        {
            int lockValue = getLockVal(blockId);
            if (lockValue > 1)
            {
                if (locks_.TryUpdate(blockId, lockValue - 1, lockValue))
                {
                    //ok
                    success = true;
                }
                else
                {
                    //someone changed value or deleted
                }
            }
            else
            {
                if (locks_.TryRemove(new KeyValuePair<BlockId, int>(blockId, 1)))
                {
                    //ok
                    success = true;
                }
                else
                {
                    //someone changed value or deleted
                }
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
