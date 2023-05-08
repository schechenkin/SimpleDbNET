using System.Collections.Concurrent;
using SimpleDb.File;

namespace Playground.LockTableTest;

public class LockTableWithConcurrentDictionary : ILockTable
{
    private ConcurrentDictionary<BlockId, int> locks_ = new ConcurrentDictionary<BlockId, int>();

    private static TimeSpan MAX_WAIT_TIME = TimeSpan.FromSeconds(10);

    public void WaitExclusiveLock(in BlockId blockId)
    {
        throw new NotImplementedException();
    }

    public void WaitSharedLock(in BlockId blockId)
    {
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

