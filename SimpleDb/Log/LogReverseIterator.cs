using System.Collections;
using SimpleDb.Abstractions;
using SimpleDb.File;

namespace SimpleDb.Log;

public class LogReverseIterator : IEnumerable
{
    private IFileManager fileManager_;
    private BlockId blockId_;
    private Page page_;
    private int currentPosition_;
    private int boundary_;

    /**
    * Creates an iterator for the records in the log file,
    * positioned after the last log record.
    */
    public LogReverseIterator(IFileManager fm, BlockId blk)
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
    internal bool HasNext()
    {
        return currentPosition_ < fileManager_.BlockSize || blockId_.Number > 0;
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
        if (currentPosition_ == fileManager_.BlockSize)
        {
            blockId_ = BlockId.New(blockId_.FileName, blockId_.Number - 1);
            MoveToBlock(blockId_);
        }
        byte[] rec = page_.GetBytesArray(currentPosition_);
        currentPosition_ += sizeof(int) + rec.Length;
        return rec;
    }

    /**
     * Moves to the specified log block
     * and positions it at the first record in that block
     * (i.e., the most recent one).
     */
    private void MoveToBlock(BlockId blk)
    {
        fileManager_.ReadPage(blk, page_);
        boundary_ = page_.GetInt(0);
        currentPosition_ = boundary_;
    }
}
