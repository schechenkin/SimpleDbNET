using SimpleDB.file;
using SimpleDB.log;

namespace SimpleDB.Data
{
    public class Buffer
    {
        private FileManager m_FileManager;
        private LogManager m_logManager;
        private int m_PinsCount = 0;
        private int m_TransactionNumber = -1;
        private int m_lsn = -1;
        public Page Page { get; private set; }
        public BlockId BlockId { get; private set; }
        public bool IsPinned => m_PinsCount > 0;

        public Buffer(FileManager fm, LogManager lm)
        {
            this.m_FileManager = fm;
            this.m_logManager = lm;
            Page = new Page(fm.BlockSize);
        }

        public void SetModified(int txnum, int lsn)
        {
            this.m_TransactionNumber = txnum;
            if (lsn >= 0)
                this.m_lsn = lsn;
        }

        /**
         * Return true if the buffer is currently pinned
         * (that is, if it has a nonzero pin count).
         * @return true if the buffer is pinned
         */

        public int modifyingTx()
        {
            return m_TransactionNumber;
        }

        /**
         * Reads the contents of the specified block into
         * the contents of the buffer.
         * If the buffer was dirty, then its previous contents
         * are first written to disk.
         * @param b a reference to the data block
         */
        internal void AssignToBlock(BlockId blockId)
        {
            Flush();
            BlockId = blockId;
            m_FileManager.ReadBlock(BlockId, Page);
            m_PinsCount = 0;
        }

        /**
         * Write the buffer to its disk block if it is dirty.
         */
        internal void Flush()
        {
            if (m_TransactionNumber >= 0)
            {
                m_logManager.Flush(m_lsn);
                m_FileManager.WritePage(Page, BlockId);
                m_TransactionNumber = -1;
            }
        }

        /**
         * Increase the buffer's pin count.
         */
        internal void Pin()
        {
            m_PinsCount++;
        }

        /**
         * Decrease the buffer's pin count.
         */
        internal void Unpin()
        {
            m_PinsCount--;
        }
    }
}
