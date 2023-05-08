using System.Collections.Concurrent;
using SimpleDb.File;

namespace Playground.LockTableTest;

public interface ILockTable
{
    void WaitExclusiveLock(in BlockId blockId);
    void WaitSharedLock(in BlockId blockId);
    void UnLock(in BlockId blockId);
}


public class LockTableWithReadWriteLock : ILockTable
{
    ConcurrentDictionary<BlockId, ReaderWriterLock> locks = new();

    public void WaitExclusiveLock(in BlockId blockId)
    {
        ReaderWriterLock rwl = locks.GetOrAdd(blockId, (blockId) => new ReaderWriterLock());
        rwl.AcquireWriterLock(100);
    }

    public void WaitSharedLock(in BlockId blockId)
    {
        ReaderWriterLock rwl = locks.GetOrAdd(blockId, (blockId) => new ReaderWriterLock());
        rwl.AcquireReaderLock(100);
    }

    public void UnLock(in BlockId blockId)
    {
        ReaderWriterLock? rwl;
        if(locks.TryGetValue(blockId, out rwl))
        {
            rwl.ReleaseLock();
        }
    }
}

