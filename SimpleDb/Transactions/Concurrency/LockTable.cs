using SimpleDB.file;
using System.Collections;
using System.Runtime.CompilerServices;

namespace SimpleDb.Transactions.Concurrency
{
    public class LockTable
    {
        private static TimeSpan MAX_WAIT_TIME = TimeSpan.FromSeconds(10);

        private Hashtable locks = new Hashtable();

        /**
         * Grant an SLock on the specified block.
         * If an XLock exists when the method is called,
         * then the calling thread will be placed on a wait list
         * until the lock is released.
         * If the thread remains on the wait list for a certain 
         * amount of time (currently 10 seconds),
         * then an exception is thrown.
         * @param blk a reference to the disk block
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void sLock(BlockId blockId)
        {
            DateTime timestamp = DateTime.Now;
            while (hasXlock(blockId) && !WaitingTooLong(timestamp))
                Thread.Sleep(100);
            if (hasXlock(blockId))
                throw new LockAbortException();
            int lockValue = getLockVal(blockId);  // will not be negative

            if (!locks.ContainsKey(blockId))
                locks.Add(blockId, lockValue + 1);
            else
                locks[blockId] = lockValue + 1;
        }

        /**
         * Grant an XLock on the specified block.
         * If a lock of any type exists when the method is called,
         * then the calling thread will be placed on a wait list
         * until the locks are released.
         * If the thread remains on the wait list for a certain 
         * amount of time (currently 10 seconds),
         * then an exception is thrown.
         * @param blk a reference to the disk block
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void xLock(BlockId blockId)
        {
            DateTime timestamp = DateTime.Now;
            while (hasOtherSLocks(blockId) && !WaitingTooLong(timestamp))
                Thread.Sleep(100);
            if (hasOtherSLocks(blockId))
                throw new LockAbortException();

            if (!locks.ContainsKey(blockId))
                locks.Add(blockId, -1);
            else
                locks[blockId] = -1;
        }

        /**
         * Release a lock on the specified block.
         * If this lock is the last lock on that block,
         * then the waiting transactions are notified.
         * @param blk a reference to the disk block
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void unlock(BlockId blockId)
        {
            int lockValue = getLockVal(blockId);
            if (lockValue > 1)
                locks[blockId] = lockValue - 1;
            else
            {
                locks.Remove(blockId);
                //notifyAll();
            }
        }

        private bool hasXlock(BlockId blockId)
        {
            return getLockVal(blockId) < 0;
        }

        private bool hasOtherSLocks(BlockId blockId)
        {
            return getLockVal(blockId) > 1;
        }

        private bool WaitingTooLong(DateTime starttime)
        {
            return DateTime.Now - starttime > MAX_WAIT_TIME;
        }

        private int getLockVal(BlockId blk)
        {
            if (locks.ContainsKey(blk))
                return (int)locks[blk];

            return 0;
        }
    }
}
