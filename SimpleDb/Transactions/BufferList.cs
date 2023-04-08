using SimpleDb.Buffers;
using SimpleDb.File;

namespace SimpleDb.Transactions;

    internal class BufferList
    {
        private Dictionary<BlockId, SimpleDb.Buffers.Buffer> buffers = new Dictionary<BlockId, SimpleDb.Buffers.Buffer>();
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
        internal SimpleDb.Buffers.Buffer getBuffer(BlockId blk)
        {
            return buffers[blk];
        }

        /**
         * Pin the block and keep track of the buffer internally.
         * @param blk a reference to the disk block
         */
        internal void pin(in BlockId blockId)
        {
            SimpleDb.Buffers.Buffer buff = bm.PinBlock(blockId);
            buffers[blockId] = buff;
            pins.Add(blockId);
        }

        /**
         * Unpin the specified block.
         * @param blk a reference to the disk block
         */
        internal void unpin(in BlockId blockId)
        {
            SimpleDb.Buffers.Buffer buffer = buffers[blockId];
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
                SimpleDb.Buffers.Buffer buff = buffers[blk];
                bm.UnpinBuffer(buff);
            }
            buffers.Clear();
            pins.Clear();
        }
    }