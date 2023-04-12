using SimpleDb.File;

namespace SimpleDb.Transactions.Concurrency;

public class ConcurrencyManager
{
    private LockTable lockTable;
    private Dictionary<BlockId, char> locks = new Dictionary<BlockId, char>();

    public ConcurrencyManager(LockTable lockTable)
    {
        this.lockTable = lockTable;
    }

    internal void Release()
    {
        return;
        
        foreach (BlockId blockId in locks.Keys)
            lockTable.UnLock(blockId);

        locks.Clear();
    }

    internal void RequestSharedLock(in BlockId blockId)
    {
        return;
        
        if (!locks.ContainsKey(blockId))
        {
            lockTable.WaitSharedLock(blockId);
            locks[blockId] = 'S';
        }
    }

    internal void RequestExclusiveLock(in BlockId blockId)
    {       
        return;
        
        if (!HasXLock(blockId))
        {
            RequestSharedLock(blockId);
            lockTable.WaitExclusiveLock(blockId);
            locks[blockId] = 'X';
        }
    }

    private bool HasXLock(in BlockId blockId)
    {
        if (!locks.ContainsKey(blockId))
            return false;

        return locks[blockId] == 'X';
    }
}
