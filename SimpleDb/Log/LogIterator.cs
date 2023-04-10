using System.Collections;
using SimpleDb.Abstractions;
using SimpleDb.File;

namespace SimpleDb.Log;

public class LogIterator : IEnumerable
{
    private IFileManager fileManager_;
    private BlockId blockId_;
    private Page page_;
    private int currentPosition_;
    private int boundary;
    private List<int> recordsPositions_ = new List<int>();
    private int currentRecordPositionIndex_ = 0;

    /**
    * Creates an iterator for the records in the log file,
    * positioned after the first log record.
    */
    public LogIterator(IFileManager fm, BlockId blk)
    {
        this.fileManager_ = fm;
        this.blockId_ = blk;
        byte[] b = new byte[fm.BlockSize];
        page_ = new Page(b);
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
    public bool HasNext()
    {
        return currentRecordPositionIndex_ < recordsPositions_.Count || !atLastBlock();
    }

    /**
     * Moves to the next log record in the block.
     * If there are no more log records in the block,
     * then move to the previous block
     * and return the log record from there.
     * @return the next earliest log record
     */
    public byte[] Next()
    {
        if (currentRecordPositionIndex_ == recordsPositions_.Count)
        {
            blockId_ = BlockId.New(blockId_.FileName, blockId_.Number + 1);
            MoveToBlock(blockId_);
        }

        byte[] rec = page_.GetBytesArray(recordsPositions_[currentRecordPositionIndex_]);
        currentRecordPositionIndex_++;
        if (currentRecordPositionIndex_ < recordsPositions_.Count)
            currentPosition_ = recordsPositions_[currentRecordPositionIndex_];
        return rec;
    }

    private void MoveToBlock(BlockId blk)
    {
        fileManager_.ReadPage(blk, page_);
        boundary = page_.GetInt(0);
        currentPosition_ = boundary;

        recordsPositions_ = new List<int>();
        while (currentPosition_ < fileManager_.BlockSize)
        {
            recordsPositions_.Add(currentPosition_);
            int recordLength = page_.GetInt(currentPosition_);
            currentPosition_ += sizeof(int) + recordLength;
        }
        recordsPositions_.Reverse();
        currentRecordPositionIndex_ = 0;

        currentPosition_ = boundary;
    }

    private bool atLastBlock()
    {
        return blockId_.Number == fileManager_.GetBlocksCount(blockId_.FileName) - 1;
    }
}
