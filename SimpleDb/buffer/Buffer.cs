using SimpleDB.file;
using SimpleDB.log;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.Data
{
    public class Buffer
    {
        private FileMgr fm;
        private LogMgr lm;
        private Page _contents;
        private BlockId blk = null;
        private int pins = 0;
        private int txnum = -1;
        private int lsn = -1;

        public Buffer(FileMgr fm, LogMgr lm)
        {
            this.fm = fm;
            this.lm = lm;
            _contents = new Page(fm.blockSize());
        }

        public Page contents()
        {
            return _contents;
        }

        /**
         * Returns a reference to the disk block
         * allocated to the buffer.
         * @return a reference to a disk block
         */
        public BlockId block()
        {
            return blk;
        }

        public void setModified(int txnum, int lsn)
        {
            this.txnum = txnum;
            if (lsn >= 0)
                this.lsn = lsn;
        }

        /**
         * Return true if the buffer is currently pinned
         * (that is, if it has a nonzero pin count).
         * @return true if the buffer is pinned
         */
        public bool isPinned()
        {
            return pins > 0;
        }

        public int modifyingTx()
        {
            return txnum;
        }

        /**
         * Reads the contents of the specified block into
         * the contents of the buffer.
         * If the buffer was dirty, then its previous contents
         * are first written to disk.
         * @param b a reference to the data block
         */
        internal void assignToBlock(BlockId b)
        {
            flush();
            blk = b;
            fm.read(blk, _contents);
            pins = 0;
        }

        /**
         * Write the buffer to its disk block if it is dirty.
         */
        internal void flush()
        {
            if (txnum >= 0)
            {
                lm.flush(lsn);
                fm.write(blk, _contents);
                txnum = -1;
            }
        }

        /**
         * Increase the buffer's pin count.
         */
        internal void pin()
        {
            pins++;
        }

        /**
         * Decrease the buffer's pin count.
         */
        internal void unpin()
        {
            pins--;
        }
    }
}
