using SimpleDB.file;
using SimpleDB.Record;
using System.Collections;

namespace SimpleDB.log
{
    internal class LogReverseIterator : IEnumerable
    {
        private FileManager fm;
        private BlockId blk;
        private Page page;
        private int currentpos;
        private int boundary;
        private List<int> recordsPositions = new List<int>();
        private int currentRecordPositionIndex = 0;

        /**
        * Creates an iterator for the records in the log file,
        * positioned after the first log record.
        */
        public LogReverseIterator(FileManager fm, BlockId blk)
        {
            this.fm = fm;
            this.blk = blk;
            byte[] b = new byte[fm.BlockSize];
            page = new Page(b);
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
            return currentRecordPositionIndex < recordsPositions.Count || !atLastBlock();
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
            if (currentRecordPositionIndex == recordsPositions.Count)
            {
                blk = BlockId.New(blk.FileName, blk.Number + 1);
                MoveToBlock(blk);
            }

            byte[] rec = page.GetBytesArray(recordsPositions[currentRecordPositionIndex]);
            currentRecordPositionIndex++;
            if(currentRecordPositionIndex < recordsPositions.Count)
                currentpos = recordsPositions[currentRecordPositionIndex];
            return rec;
        }

        private void MoveToBlock(BlockId blk)
        {
            fm.ReadBlock(blk, page);
            boundary = page.GetInt(0);
            currentpos = boundary;

            recordsPositions = new List<int>();
            while(currentpos < fm.BlockSize)
            {
                recordsPositions.Add(currentpos);
                int recordLength = page.GetInt(currentpos);
                currentpos += sizeof(int) + recordLength;
            }
            recordsPositions.Reverse();
            currentRecordPositionIndex = 0;

            currentpos = boundary;
        }

        private bool atLastBlock()
        {
            return blk.Number == fm.GetBlocksCount(blk.FileName) - 1;
        }
    }
}
