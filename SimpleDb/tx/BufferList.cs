using SimpleDB.Data;
using SimpleDB.file;
using Buffer = SimpleDB.Data.Buffer;

namespace SimpleDB.Tx
{
    internal class BufferList
    {
        private Dictionary<BlockId, Buffer> buffers = new Dictionary<BlockId, Buffer>();
        private List<BlockId> pins = new List<BlockId>();
        private BufferMgr bm;

        public BufferList(BufferMgr bm)
        {
            this.bm = bm;
        }

        /**
         * Return the buffer pinned to the specified block.
         * The method returns null if the transaction has not
         * pinned the block.
         * @param blk a reference to the disk block
         * @return the buffer pinned to that block
         */
        internal Buffer getBuffer(BlockId blk)
        {
            return buffers[blk];
        }

        /**
         * Pin the block and keep track of the buffer internally.
         * @param blk a reference to the disk block
         */
        internal void pin(BlockId blk)
        {
            Buffer buff = bm.pin(blk);
            buffers[blk] = buff;
            pins.Add(blk);
        }

        /**
         * Unpin the specified block.
         * @param blk a reference to the disk block
         */
        internal void unpin(BlockId blk)
        {
            Buffer buff = buffers[blk];
            bm.unpin(buff);
            pins.Remove(blk);
            if (!pins.Contains(blk))
                buffers.Remove(blk);
        }

        /**
         * Unpin any buffers still pinned by this transaction.
         */
        internal void unpinAll()
        {
            foreach (BlockId blk in pins)
            {
                Buffer buff = buffers[blk];
                bm.unpin(buff);
            }
            buffers.Clear();
            pins.Clear();
        }
    }
}
