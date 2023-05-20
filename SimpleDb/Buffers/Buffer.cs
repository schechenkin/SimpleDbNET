using SimpleDb.File;
using SimpleDb.Abstractions;
using System.Diagnostics;

namespace SimpleDb.Buffers;

public class Buffer
{
    IFileManager fileManager_;
    ILogManager logManager_;
    long pinsCount_ = 0;
    long usageCount_ = 0;
    TransactionNumber? transactionNumber_;
    LSN? lsn_;

    public Page Page { get; private set; }
    public BlockId? BlockId { get; private set; }
    public bool IsPinned => Interlocked.Read(ref pinsCount_) > 0;
    public long UsageCount => Interlocked.Read(ref usageCount_);
    public bool IsDirty => !IsPinned && ModifiedByTransaction().HasValue;

    public Buffer(IFileManager fileManager, ILogManager logManager)
    {
        fileManager_ = fileManager;
        logManager_ = logManager;
        Page = new Page(fileManager.BlockSize);
    }

    public void SetModified(in TransactionNumber txnum, in LSN lsn)
    {
        Debug.Assert(BlockId.HasValue);
        this.transactionNumber_ = txnum;
        if (lsn >= 0)
            this.lsn_ = lsn;
    }

    public TransactionNumber? ModifiedByTransaction()
    {
        return transactionNumber_;
    }

    internal void AssignToBlock(in BlockId blockId)
    {
        Flush();
        BlockId = blockId;
        fileManager_.ReadPage(blockId, Page);
        pinsCount_ = 0;
    }

    /**
     * Write the buffer to its disk block if it is dirty.
     */
    internal void Flush(bool forceDiskWrite = false)
    {
        if (transactionNumber_ is not null)
        {
            if(lsn_.HasValue)
            {
                logManager_.Flush(lsn_.Value, forceDiskWrite);
            }
            Debug.Assert(BlockId.HasValue);
            fileManager_.WritePage(BlockId.Value, Page, forceDiskWrite);
            transactionNumber_ = null;
        }
    }

    /**
     * Increase the buffer's pin count.
     */
    internal void Pin()
    {
        Interlocked.Increment(ref pinsCount_);
    }

    internal void IncrementUsageCounter()
    {
        if (UsageCount < 5)
            Interlocked.Increment(ref usageCount_);
    }

    internal void DecrementUsageCounter()
    {
        if (UsageCount > 0)
            Interlocked.Decrement(ref usageCount_);
    }

    internal void Unpin()
    {
        Interlocked.Decrement(ref pinsCount_);
    }

    public override string ToString()
    {
        return $"BlockId={BlockId}, IsPinned={IsPinned}, lsn={lsn_}, tid={transactionNumber_}";
    }
}