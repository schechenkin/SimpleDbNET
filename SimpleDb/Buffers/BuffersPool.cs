using SimpleDb.Abstractions;
using System.Diagnostics;

namespace SimpleDb.Buffers
{
    internal class BuffersPool
    {
        private List<Buffer> buffers;
        private ReaderWriterLockSlim buffersLock = new ReaderWriterLockSlim();

        public BuffersPool(int numbuffs)
        {
            buffers = new List<Buffer>(numbuffs);
        }

        internal void Add(Buffer buffer)
        {
            buffersLock.EnterWriteLock();
            try
            {
                buffers.Add(buffer);
            }
            finally
            {
                buffersLock.ExitWriteLock();
            }
        }

        public Buffer this[int index]
        {
            get 
            { 
                Debug.Assert(index < buffers.Count);
                return buffers[index]; 
            }
        }

        public int Count => buffers.Count;

        public int GetUnpinnedBlocksCount()
        {
            buffersLock.EnterReadLock();
            try
            {
                return buffers.Where(x => !x.IsPinned).Count();
            }
            finally
            {
                buffersLock.ExitReadLock();
            }
        }

        public List<Buffer> GetDirtyBuffers()
        {
            buffersLock.EnterReadLock();
            try
            {
                return buffers.Where(buffer => buffer.IsDirty).ToList();
            }
            finally
            {
                buffersLock.ExitReadLock();
            }
        }

        public int GetDirtyBlocksCount()
        {
            buffersLock.EnterReadLock();
            try
            {
                return buffers.Where(x => !x.IsPinned && x.ModifiedByTransaction().HasValue).Count();
            }
            finally
            {
                buffersLock.ExitReadLock();
            }
        }

        public List<Buffer> GetBuffersModifiedBy(in TransactionNumber txnum)
        {
            buffersLock.EnterReadLock();
            try
            {
                List<Buffer> result = new List<Buffer>();
                foreach (Buffer buff in buffers)
                    if (buff.ModifiedByTransaction() == txnum)
                        result.Add(buff);

                return result;
            }
            finally
            {
                buffersLock.ExitReadLock();
            }
        }

        public Dictionary<string, int> GetUsageByFiles()
        {
            buffersLock.EnterReadLock();
            try
            {
                Dictionary<string, int> blocksCount = new();

                foreach (var group in buffers.Where(b => b.BlockId != null).GroupBy(b => b.BlockId?.FileName))
                {
                    if (group.Key != null)
                        blocksCount.Add(group.Key, group.Count());
                }


                return blocksCount;
            }
            finally
            {
                buffersLock.ExitReadLock();
            }
        }

        public void PrintBufferPool()
        {
            Console.WriteLine("buffers:");

            buffersLock.EnterReadLock();
            try
            {
                foreach (var buffer in buffers)
                {
                    Console.WriteLine(buffer.ToString());
                }
            }
            finally
            {
                buffersLock.ExitReadLock();
            }
        }
    }
}
