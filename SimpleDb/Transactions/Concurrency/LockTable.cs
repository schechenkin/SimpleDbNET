using System.Collections.Concurrent;
using SimpleDb.File;
namespace SimpleDb.Transactions.Concurrency;

public class LockTable
{
    ConcurrentDictionary<BlockId, ReaderWriterLock> locks = new();

    public void WaitExclusiveLock(in BlockId blockId)
    {
        return;
        
        ReaderWriterLock rwl = locks.GetOrAdd(blockId, (blockId) => new ReaderWriterLock());
        rwl.AcquireWriterLock(100);
    }

    public void WaitSharedLock(in BlockId blockId)
    {
        return;
        
        ReaderWriterLock rwl = locks.GetOrAdd(blockId, (blockId) => new ReaderWriterLock());
        rwl.AcquireReaderLock(100);
    }

    public void UnLock(in BlockId blockId)
    {
        return;
        
        ReaderWriterLock? rwl;
        if (locks.TryGetValue(blockId, out rwl))
        {
            rwl.ReleaseLock();
        }
    }
}
