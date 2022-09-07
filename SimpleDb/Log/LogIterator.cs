using SimpleDB.file;
using System.Collections;

namespace SimpleDB.log
{
    internal class LogIterator : IEnumerable
    {
        private FileManager fm;
        private BlockId blk;
        private Page p;
        private int currentpos;
        private int boundary;

        /**
        * Creates an iterator for the records in the log file,
        * positioned after the last log record.
        */
        public LogIterator(FileManager fm, BlockId blk)
        {
            this.fm = fm;
            this.blk = blk;
            byte[] b = new byte[fm.BlockSize];
            p = new Page(b);
            MoveToBlock(blk);
        }

        public IEnumerator GetEnumerator()
        {
            while (HasNext())
            {
                yield return Next();
            }
        }

        /**
         * Determines if the current log record
         * is the earliest record in the log file.
         * @return true if there is an earlier record
         */
        internal bool HasNext()
        {
            return currentpos < fm.BlockSize || blk.Number > 0;
        }

        /**
         * Moves to the next log record in the block.
         * If there are no more log records in the block,
         * then move to the previous block
         * and return the log record from there.
         * @return the next earliest log record
         */
        internal byte[] Next()
        {
            if (currentpos == fm.BlockSize)
            {
                blk = BlockId.New(blk.FileName, blk.Number - 1);
                MoveToBlock(blk);
            }
            byte[] rec = p.GetBytesArray(currentpos);
            currentpos += sizeof(int) + rec.Length;
            return rec;
        }

        /**
         * Moves to the specified log block
         * and positions it at the first record in that block
         * (i.e., the most recent one).
         */
        private void MoveToBlock(BlockId blk)
        {
            fm.ReadBlock(blk, p);
            boundary = p.GetInt(0);
            currentpos = boundary;
        }
    }
}
