using System.Collections.Concurrent;
using SimpleDb.Abstractions;
using SimpleDb.File;

namespace SimpleDb.Buffers;

public class BufferManager
{
    private ConcurrentBag<Buffer> m_bufferpool;
    private ConcurrentDictionary<BlockId, Buffer> m_blockToBufferMap = new ();
    private object mutex = new object();
    private static TimeSpan MAX_WAIT_TIME = new TimeSpan(0, 0, 10); // 10 seconds
    private FreeList_ThreadSafe m_freeList;
    private long m_clockSweepCurrentIndex = 0;

    /**
     * Creates a buffer manager having the specified number 
     * of buffer slots.
     * This constructor depends on a {@link FileMgr} and
     * {@link simpledb.log.LogMgr LogMgr} object.
     * @param numbuffs the number of buffer slots to allocate
     */
    public BufferManager(IFileManager fm, ILogManager lm, int numbuffs)
    {
        m_freeList = new FreeList_ThreadSafe(numbuffs, fm, lm);
        m_bufferpool = new ConcurrentBag<Buffer>();
    }

    /**
     * Flushes the dirty buffers modified by the specified transaction.
     * @param txnum the transaction's id number
     */
    public void FlushAll(in TransactionNumber txnum)
    {
        lock (mutex)
        {
            foreach (Buffer buff in m_bufferpool)
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
        buffer.Unpin();
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
        DateTime timestamp = DateTime.Now;
        var buff = TryPinBlock(blockId);
        while (buff == null && !WaitingTooLong(timestamp))
        {
            buff = TryPinBlock(blockId);
            if(buff is null)
                Thread.Yield();
        }
        if (buff == null)
            throw new BufferAbortException();
        return buff;

    }

    private bool WaitingTooLong(DateTime startWaitingTime)
    {
        return DateTime.Now - startWaitingTime > MAX_WAIT_TIME;
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
            buffer = ChooseBufferFromFreeList();//ts

            if (buffer is null)
            {
                buffer = ChooseUnpinnedBuffer(); //ts?
                if (buffer is not null)
                    RemoveLinkFromBlockToBufferMap(buffer);//ts
            }
            else
                m_bufferpool.Add(buffer);//ts

            if (buffer == null)
                return null;

            if(m_blockToBufferMap.TryAdd(blockId, buffer))
            {
                buffer.AssignToBlock(blockId);
            }
            else
                return null;
        }

        buffer.Pin();//ts
        buffer.IncrementUsageCounter();//ts
        return buffer;
    }

    private void RemoveLinkFromBlockToBufferMap(Buffer buffer)
    {
        if (buffer.BlockId.HasValue)
        {
            if(!m_blockToBufferMap.TryRemove(new KeyValuePair<BlockId, Buffer>(buffer.BlockId.Value, buffer)))
                throw new Exception("error while remove buffer from blockToBufferMap");
        }
    }

    private Buffer? FindBufferContainsBlock(in BlockId blockId)
    {
        Buffer? buffer = null;
        if (m_blockToBufferMap.TryGetValue(blockId, out buffer))
            return buffer;
        else
            return null;
    }

    private Buffer? ChooseBufferFromFreeList()
    {
        Buffer? buffer;

        if (m_freeList.TryGetBuffer(out buffer))
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
            if (m_clockSweepCurrentIndex >= m_bufferpool.Count)
                m_clockSweepCurrentIndex = 0;

            bool anyFound = false;
            while (m_clockSweepCurrentIndex < m_bufferpool.Count)
            {               
                Buffer buffer = m_bufferpool.ElementAt((int)m_clockSweepCurrentIndex);

                if (!buffer.IsPinned)
                {
                    if (buffer.UsageCount == 0)
                        return buffer;
                    else
                        anyFound = true;
                }

                buffer.DecrementUsageCounter();

                Interlocked.Increment(ref m_clockSweepCurrentIndex);
            }

            if (anyFound == false)
                return null;
        }
    }

    public int GetUnpinnedBlocksCount()
    {
        return m_bufferpool.Where(x => !x.IsPinned).Count();
    }

    public int GetFreeBlockCount()
    {
        return m_freeList.BufferCount;
    }

    public UsageStats GetUsageStats()
    {
        lock (mutex)
        {
            Dictionary<string, int> blocksCount = new();

            foreach (var group in m_bufferpool.Where(b => b.BlockId != null).GroupBy(b => b.BlockId?.FileName))
            {
                if (group.Key != null)
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

    private void PrintBufferPool()
    {
        Console.WriteLine("buffers:");
        foreach (var buffer in m_bufferpool)
        {
            Console.WriteLine(buffer.ToString());
        }
    }

    public void Print(bool printBufferPool = true)
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
        if (printBufferPool)
            PrintBufferPool();
    }

    public class UsageStats
    {
        public int FreeBlockCount { get; set; }
        public int UnpinnedBlockCount { get; set; }
        public Dictionary<string, int> BlocksCount { get; set; } = new Dictionary<string, int>();
    }
}