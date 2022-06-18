using SimpleDb.Transactions.Concurrency;
using SimpleDB.file;
using System.Collections;

namespace SimpleDB.tx.concurrency
{
    public class ConcurrencyManager
    {
        private static LockTable lockTable = new LockTable();
        private Hashtable locks = new Hashtable();

        internal void Release()
        {
            foreach (BlockId blockId in locks.Keys)
                lockTable.unlock(blockId);

            locks.Clear();
        }

        internal void RequestSharedLock(BlockId blockId)
        {
            if (!locks.ContainsKey(blockId))
            {
                lockTable.sLock(blockId);
                locks[blockId] = 'S';
            }
        }

        internal void RequestExclusiveLock(BlockId blockId)
        {
            if (!HasXLock(blockId))
            {
                RequestSharedLock(blockId);
                lockTable.xLock(blockId);
                locks[blockId] = 'X';
            }
        }

        private bool HasXLock(BlockId blockId)
        {
            if (!locks.ContainsKey(blockId))
                return false;

            return (char)locks[blockId] == 'X';
        }
    }
}
