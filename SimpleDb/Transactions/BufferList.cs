using SimpleDB.Data;
using SimpleDB.file;
using Buffer = SimpleDB.Data.Buffer;

namespace SimpleDB.Tx
{
    internal class BufferList
    {
        private Dictionary<BlockId, Buffer> buffers = new Dictionary<BlockId, Buffer>();
        private List<BlockId> pins = new List<BlockId>();
        private BufferManager bm;

        public BufferList(BufferManager bm)
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
        internal void pin(in BlockId blockId)
        {
            Buffer buff = bm.PinBlock(blockId);
            buffers[blockId] = buff;
            pins.Add(blockId);
        }

        /**
         * Unpin the specified block.
         * @param blk a reference to the disk block
         */
        internal void unpin(in BlockId blockId)
        {
            Buffer buffer = buffers[blockId];
            bm.UnpinBuffer(buffer);
            pins.Remove(blockId);
            if (!pins.Contains(blockId))
                buffers.Remove(blockId);
        }

        /**
         * Unpin any buffers still pinned by this transaction.
         */
        internal void unpinAll()
        {
            foreach (BlockId blk in pins)
            {
                Buffer buff = buffers[blk];
                bm.UnpinBuffer(buff);
            }
            buffers.Clear();
            pins.Clear();
        }
    }
}
