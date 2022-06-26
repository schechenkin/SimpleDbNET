using SimpleDb.Transactions.Concurrency;
using SimpleDB.file;
using System.Collections;

namespace SimpleDB.tx.concurrency
{
    public class ConcurrencyManager
    {
        private LockTable lockTable;
        private Hashtable locks = new Hashtable();

        public ConcurrencyManager(LockTable lockTable)
        {
            this.lockTable = lockTable;
        }

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
