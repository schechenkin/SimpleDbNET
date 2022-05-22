using SimpleDB.file;
using SimpleDB.log;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleDB.Data
{
    public class BufferMgr
    {
        private Buffer[] bufferpool;
        private int numAvailable;
        private Mutex mutex = new Mutex();
        private static TimeSpan MAX_TIME = new TimeSpan(0, 0, 10); // 10 seconds

        /**
         * Creates a buffer manager having the specified number 
         * of buffer slots.
         * This constructor depends on a {@link FileMgr} and
         * {@link simpledb.log.LogMgr LogMgr} object.
         * @param numbuffs the number of buffer slots to allocate
         */
        public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs)
        {
            bufferpool = new Buffer[numbuffs];
            numAvailable = numbuffs;
            for (int i = 0; i < numbuffs; i++)
                bufferpool[i] = new Buffer(fm, lm);
        }

        /**
         * Returns the number of available (i.e. unpinned) buffers.
         * @return the number of available buffers
         */
        public int available()
        {
            lock(mutex)
            {
                return numAvailable;
            }
        }

        /**
         * Flushes the dirty buffers modified by the specified transaction.
         * @param txnum the transaction's id number
         */
        public void flushAll(int txnum)
        {
            lock (mutex)
            {
                foreach (Buffer buff in bufferpool)
                    if (buff.modifyingTx() == txnum)
                        buff.flush();
            }
        }


        /**
         * Unpins the specified data buffer. If its pin count
         * goes to zero, then notify any waiting threads.
         * @param buff the buffer to be unpinned
         */
        public void unpin(Buffer buff)
        {
            lock (mutex)
            {
                buff.unpin();
                if (!buff.isPinned())
                {
                    numAvailable++;
                    //notifyAll();
                }
            }
        }

        /**
         * Pins a buffer to the specified block, potentially
         * waiting until a buffer becomes available.
         * If no buffer becomes available within a fixed 
         * time period, then a {@link BufferAbortException} is thrown.
         * @param blk a reference to a disk block
         * @return the buffer pinned to that block
         */
        public Buffer pin(BlockId blk)
        {
            lock (mutex)
            {
                DateTime timestamp = DateTime.Now;
                Buffer buff = tryToPin(blk);
                while (buff == null && !waitingTooLong(timestamp))
                {
                    Thread.Sleep(100);
                    buff = tryToPin(blk);
                }
                if (buff == null)
                    throw new BufferAbortException();
                return buff;
            }
        }

        private bool waitingTooLong(DateTime starttime)
        {
            return DateTime.Now - starttime > MAX_TIME;
        }

        /**
         * Tries to pin a buffer to the specified block. 
         * If there is already a buffer assigned to that block
         * then that buffer is used;  
         * otherwise, an unpinned buffer from the pool is chosen.
         * Returns a null value if there are no available buffers.
         * @param blk a reference to a disk block
         * @return the pinned buffer
         */
        private Buffer tryToPin(BlockId blk)
        {
            Buffer buff = findExistingBuffer(blk);
            if (buff == null)
            {
                buff = chooseUnpinnedBuffer();
                if (buff == null)
                    return null;
                buff.assignToBlock(blk);
            }
            if (!buff.isPinned())
                numAvailable--;
            buff.pin();
            return buff;
        }

        private Buffer findExistingBuffer(BlockId blk)
        {
            foreach (Buffer buff in bufferpool)
            {
                BlockId b = buff.block();
                if (b != null && b.Equals(blk))
                    return buff;
            }
            return null;
        }

        private Buffer chooseUnpinnedBuffer()
        {
            foreach (Buffer buff in bufferpool)
                if (!buff.isPinned())
                    return buff;
            return null;
        }
    }
}
