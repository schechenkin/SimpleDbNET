using SimpleDb.Abstractions;
using SimpleDb.File;

namespace SimpleDb.Buffers;

public class BufferManager
{
    private List<Buffer> m_Bufferpool;
    private Dictionary<BlockId, Buffer> m_BlockToBufferMap = new Dictionary<BlockId, Buffer>();
    private object mutex = new object();
    private static TimeSpan MAX_WAIT_TIME = new TimeSpan(0, 0, 10); // 10 seconds
    private FreeList m_FreeList;
    private int clockSweepCurrentIndex = 0;

    /**
     * Creates a buffer manager having the specified number 
     * of buffer slots.
     * This constructor depends on a {@link FileMgr} and
     * {@link simpledb.log.LogMgr LogMgr} object.
     * @param numbuffs the number of buffer slots to allocate
     */
    public BufferManager(IFileManager fm, ILogManager lm, int numbuffs)
    {
        m_FreeList = new FreeList(numbuffs, fm, lm);
        m_Bufferpool = new List<Buffer>(numbuffs);
    }

    /**
     * Flushes the dirty buffers modified by the specified transaction.
     * @param txnum the transaction's id number
     */
    public void FlushAll(in TransactionNumber txnum)
    {
        lock (mutex)
        {
            foreach (Buffer buff in m_Bufferpool)
                if (buff.ModifiedByTransaction() == txnum)
                    buff.Flush();
        }
    }


    /**
     * Unpins the specified data buffer. If its pin count
     * goes to zero, then notify any waiting threads.
     * @param buff the buffer to be unpinned
     */
    public void UnpinBuffer(Buffer buffer)
    {
        lock (mutex)
        {
            buffer.Unpin();
            if (!buffer.IsPinned)
            {
                //notifyAll();
            }
        }
    }

    /**
     * Pins a buffer to the specified block, potentially
     * waiting until a buffer becomes available.
     * If no buffer becomes available within a fixed 
     * time period, then a {@link BufferAbortException} is thrown.
     * @param blk a reference to a disk block
     * @return the buffer pinned to that block
     */
    public Buffer PinBlock(in BlockId blockId)
    {
        lock (mutex)
        {
            DateTime timestamp = DateTime.Now;
            var buff = TryPinBlock(blockId);
            while (buff == null && !WaitingTooLong(timestamp))
            {
                Thread.Sleep(100);
                buff = TryPinBlock(blockId);
            }
            if (buff == null)
                throw new BufferAbortException();
            return buff;
        }
    }

    private bool WaitingTooLong(DateTime starttime)
    {
        return DateTime.Now - starttime > MAX_WAIT_TIME;
    }

    /**
     * Tries to pin a buffer to the specified block. 
     * If there is already a buffer assigned to that block
     * then that buffer is used;  
     * otherwise, an unpinned buffer from the pool is chosen.
     * Returns a null value if there are no available buffers.
     * @param blk a reference to a disk block
     * @return the pinned buffer
     */
    private Buffer? TryPinBlock(in BlockId blockId)
    {
        var buffer = FindBufferContainsBlock(blockId);
        if (buffer == null)
        {
            buffer = ChooseBufferFromFreeList();

            if (buffer is null)
            {
                buffer = ChooseUnpinnedBuffer();
                if (buffer is not null)
                    RemoveLinkFromBlockToBufferMap(buffer);
            }
            else
                m_Bufferpool.Add(buffer);

            if (buffer == null)
                return null;

            buffer.AssignToBlock(blockId);
            m_BlockToBufferMap[blockId] = buffer;
        }

        buffer.Pin();
        buffer.IncrementUsageCounter();
        return buffer;
    }

    private void RemoveLinkFromBlockToBufferMap(Buffer buffer)
    {
        if (buffer.BlockId.HasValue)
        {
            m_BlockToBufferMap.Remove(buffer.BlockId.Value);
        }
    }

    private Buffer? FindBufferContainsBlock(in BlockId blockId)
    {
        Buffer? buffer = null;
        if (m_BlockToBufferMap.TryGetValue(blockId, out buffer))
            return buffer;
        else
            return null;
    }

    private Buffer? ChooseBufferFromFreeList()
    {
        Buffer? buffer;

        if (m_FreeList.TryGetBuffer(out buffer))
            return buffer;
        else
            return null;
    }

    private Buffer? ChooseUnpinnedBuffer()
    {
        //тут нужно в бесконечном цикле крутиться по полу буферов в поиске незапиненных буферов
        //и выходить из него только когда не нашлось ниодного незапиненного с UsageCount > 0
        while (true)
        {
            if (clockSweepCurrentIndex >= m_Bufferpool.Count)
                clockSweepCurrentIndex = 0;

            bool anyFound = false;
            while (clockSweepCurrentIndex < m_Bufferpool.Count)
            {
                Buffer buffer = m_Bufferpool[clockSweepCurrentIndex];

                if (!buffer.IsPinned)
                {
                    if (buffer.UsageCount == 0)
                        return buffer;
                    else
                        anyFound = true;
                }

                buffer.DecrementUsageCounter();

                clockSweepCurrentIndex++;
            }

            if (anyFound == false)
                return null;
        }
    }

    public int GetUnpinnedBlocksCount()
    {
        return m_Bufferpool.Where(x => !x.IsPinned).Count();
    }

    public int GetFreeBlockCount()
    {
        return m_FreeList.BufferCount;
    }

    public UsageStats GetUsageStats()
    {
        lock (mutex)
        {
            Dictionary<string, int> blocksCount = new();

            foreach (var group in m_Bufferpool.Where(b => b.BlockId != null).GroupBy(b => b.BlockId?.FileName))
            {
                if(group.Key != null)
                    blocksCount.Add(group.Key, group.Count());
            }

            UsageStats stats = new UsageStats
            {
                FreeBlockCount = GetFreeBlockCount(),
                UnpinnedBlockCount = GetUnpinnedBlocksCount(),
                BlocksCount = blocksCount
            };

            return stats;
        }
    }

    public void PrintBufferPool()
    {
        Console.WriteLine("buffers:");
        foreach (var buffer in m_Bufferpool)
        {
            Console.WriteLine(buffer.ToString());
        }
    }

    public void Print()
    {
        var usageStats = GetUsageStats();

        Console.WriteLine();
        Console.WriteLine($"stats:");
        Console.WriteLine($"FreeBlockCount {usageStats.FreeBlockCount}");
        Console.WriteLine($"UnpinnedBlockCount {usageStats.UnpinnedBlockCount}");
        foreach (var kvp in usageStats.BlocksCount)
        {
            Console.WriteLine($"Table {kvp.Key} count {kvp.Value}");
        }

        PrintBufferPool();
    }

    public class UsageStats
    {
        public int FreeBlockCount { get; set; }
        public int UnpinnedBlockCount { get; set; }
        public Dictionary<string, int> BlocksCount { get; set; } = new Dictionary<string, int>();
    }
}