using SimpleDB.file;
using SimpleDB.log;

namespace SimpleDB.Data
{
    public class BufferManager
    {
        private Buffer[] m_Bufferpool;
        private int m_AvailableBufferCounter;
        private object mutex = new object();
        private static TimeSpan MAX_WAIT_TIME = new TimeSpan(0, 0, 10); // 10 seconds

        /**
         * Creates a buffer manager having the specified number 
         * of buffer slots.
         * This constructor depends on a {@link FileMgr} and
         * {@link simpledb.log.LogMgr LogMgr} object.
         * @param numbuffs the number of buffer slots to allocate
         */
        public BufferManager(FileManager fm, LogManager lm, int numbuffs)
        {
            m_Bufferpool = new Buffer[numbuffs];
            m_AvailableBufferCounter = numbuffs;
            for (int i = 0; i < numbuffs; i++)
                m_Bufferpool[i] = new Buffer(fm, lm);
        }

        /**
         * Returns the number of available (i.e. unpinned) buffers.
         * @return the number of available buffers
         */
        public int GetAvailableBufferCount()
        {
            lock(mutex)
            {
                return m_AvailableBufferCounter;
            }
        }

        /**
         * Flushes the dirty buffers modified by the specified transaction.
         * @param txnum the transaction's id number
         */
        public void FlushAll(int txnum)
        {
            lock (mutex)
            {
                foreach (Buffer buff in m_Bufferpool)
                    if (buff.modifyingTx() == txnum)
                        buff.Flush();
            }
        }


        /**
         * Unpins the specified data buffer. If its pin count
         * goes to zero, then notify any waiting threads.
         * @param buff the buffer to be unpinned
         */
        public void UnpinBuffer(Buffer buffer)
        {
            lock (mutex)
            {
                buffer.Unpin();
                if (!buffer.IsPinned)
                {
                    m_AvailableBufferCounter++;
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
        public Buffer PinBlock(BlockId blockId)
        {
            lock (mutex)
            {
                DateTime timestamp = DateTime.Now;
                var buff = TryPinBlock(blockId);
                while (buff == null && !WaitingTooLong(timestamp))
                {
                    Thread.Sleep(100);
                    buff = TryPinBlock(blockId);
                }
                if (buff == null)
                    throw new BufferAbortException();
                return buff;
            }
        }

        private bool WaitingTooLong(DateTime starttime)
        {
            return DateTime.Now - starttime > MAX_WAIT_TIME;
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
        private Buffer? TryPinBlock(BlockId blockId)
        {
            var buffer = FindBufferContainsBlock(blockId);
            if (buffer == null)
            {
                buffer = ChooseUnpinnedBuffer();
                if (buffer == null)
                    return null;
                buffer.AssignToBlock(blockId);
            }
            if (!buffer.IsPinned)
                m_AvailableBufferCounter--;
            buffer.Pin();
            return buffer;
        }

        private Buffer? FindBufferContainsBlock(BlockId blockId)
        {
            foreach (Buffer buffer in m_Bufferpool)
            {
                BlockId b = buffer.BlockId;
                if (b != null && b.Equals(blockId))
                    return buffer;
            }
            return null;
        }

        private Buffer? ChooseUnpinnedBuffer()
        {
            foreach (Buffer buffer in m_Bufferpool)
                if (!buffer.IsPinned)
                    return buffer;
            return null;
        }
    }
}
