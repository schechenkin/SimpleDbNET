using SimpleDb.Transactions.Concurrency;
using SimpleDB.file;
using System.Collections;

namespace SimpleDB.tx.concurrency
{
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
            foreach (BlockId blockId in locks.Keys)
                lockTable.unlock(blockId);

            locks.Clear();
        }

        internal void RequestSharedLock(in BlockId blockId)
        {
            if (!locks.ContainsKey(blockId))
            {
                lockTable.sLock(blockId);
                locks[blockId] = 'S';
            }
        }

        internal void RequestExclusiveLock(in BlockId blockId)
        {
            if (!HasXLock(blockId))
            {
                RequestSharedLock(blockId);
                lockTable.xLock(blockId);
                locks[blockId] = 'X';
            }
        }

        private bool HasXLock(in BlockId blockId)
        {
            if (!locks.ContainsKey(blockId))
                return false;

            return (char)locks[blockId] == 'X';
        }
    }
}
