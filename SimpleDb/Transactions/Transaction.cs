using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.tx.concurrency;
using SimpleDB.Tx.Recovery;
using Buffer = SimpleDB.Data.Buffer;

namespace SimpleDB.Tx
{
    public class Transaction
    {
        private static int nextTransactionNumber = 0;
        private static int END_OF_FILE = -1;

        private RecoveryMgr recoveryManager;
        private ConcurrencyManager concurrencyManager;
        private BufferManager bufferManager;
        private FileManager fileManager;
        private int txNumber;
        private BufferList txBuffers;
        private static Mutex mutex = new Mutex();

        /**
         * Create a new transaction and its associated 
         * recovery and concurrency managers.
         * This constructor depends on the file, log, and buffer
         * managers that it gets from the class
         * {@link simpledb.server.SimpleDB}.
         * Those objects are created during system initialization.
         * Thus this constructor cannot be called until either
         * {@link simpledb.server.SimpleDB#init(String)} or 
         * {@link simpledb.server.SimpleDB#initFileLogAndBufferMgr(String)} or
         * is called first.
         */
        public Transaction(FileManager fileManager, LogManager logManager, BufferManager bufferManager)
        {
            this.fileManager = fileManager;
            this.bufferManager = bufferManager;
            txNumber = nextTxNumber();
            recoveryManager = new RecoveryMgr(this, txNumber, logManager, bufferManager);
            concurrencyManager = new ConcurrencyManager();
            txBuffers = new BufferList(bufferManager);
        }

        /**
         * Commit the current transaction.
         * Flush all modified buffers (and their log records),
         * write and flush a commit record to the log,
         * release all locks, and unpin any pinned buffers.
         */
        public void Commit()
        {
            recoveryManager.commit();
            concurrencyManager.Release();
            txBuffers.unpinAll();
        }

        /**
         * Rollback the current transaction.
         * Undo any modified values,
         * flush those buffers,
         * write and flush a rollback record to the log,
         * release all locks, and unpin any pinned buffers.
         */
        public void Rollback()
        {
            recoveryManager.rollback();
            concurrencyManager.Release();
            txBuffers.unpinAll();
        }

        /**
         * Flush all modified buffers.
         * Then go through the log, rolling back all
         * uncommitted transactions.  Finally, 
         * write a quiescent checkpoint record to the log.
         * This method is called during system startup,
         * before user transactions begin.
         */
        public void Recover()
        {
            bufferManager.FlushAll(txNumber);
            recoveryManager.recover();
        }

        /**
         * Pin the specified block.
         * The transaction manages the buffer for the client.
         * @param blk a reference to the disk block
         */
        public void PinBlock(BlockId blockId)
        {
            txBuffers.pin(blockId);
        }

        /**
         * Unpin the specified block.
         * The transaction looks up the buffer pinned to this block,
         * and unpins it.
         * @param blk a reference to the disk block
         */
        public void UnpinBlock(BlockId blockId)
        {
            txBuffers.unpin(blockId);
        }

        /**
         * Return the integer value stored at the
         * specified offset of the specified block.
         * The method first obtains an SLock on the block,
         * then it calls the buffer to retrieve the value.
         * @param blk a reference to a disk block
         * @param offset the byte offset within the block
         * @return the integer stored at that offset
         */
        public int GetInt(BlockId blockId, int offset)
        {
            concurrencyManager.RequestSharedLock(blockId);
            Buffer buff = txBuffers.getBuffer(blockId);
            return buff.Page.GetInt(offset);
        }

        /**
         * Return the string value stored at the
         * specified offset of the specified block.
         * The method first obtains an SLock on the block,
         * then it calls the buffer to retrieve the value.
         * @param blk a reference to a disk block
         * @param offset the byte offset within the block
         * @return the string stored at that offset
         */
        public string GetString(BlockId blockId, int offset)
        {
            concurrencyManager.RequestSharedLock(blockId);
            Buffer buffer = txBuffers.getBuffer(blockId);
            return buffer.Page.GetString(offset);
        }

        /**
         * Store an integer at the specified offset 
         * of the specified block.
         * The method first obtains an XLock on the block.
         * It then reads the current value at that offset,
         * puts it into an update log record, and 
         * writes that record to the log.
         * Finally, it calls the buffer to store the value,
         * passing in the LSN of the log record and the transaction's id. 
         * @param blk a reference to the disk block
         * @param offset a byte offset within that block
         * @param val the value to be stored
         */
        public void SetInt(BlockId blockId, int offset, int val, bool okToLog)
        {
            concurrencyManager.RequestExclusiveLock(blockId);
            Buffer buffer = txBuffers.getBuffer(blockId);
            int lsn = -1;
            if (okToLog)
                lsn = recoveryManager.setInt(buffer, offset, val);
            Page p = buffer.Page;
            p.SetInt(offset, val);
            buffer.SetModified(txNumber, lsn);
        }

        /**
         * Store a string at the specified offset 
         * of the specified block.
         * The method first obtains an XLock on the block.
         * It then reads the current value at that offset,
         * puts it into an update log record, and 
         * writes that record to the log.
         * Finally, it calls the buffer to store the value,
         * passing in the LSN of the log record and the transaction's id. 
         * @param blk a reference to the disk block
         * @param offset a byte offset within that block
         * @param val the value to be stored
         */
        public void SetString(BlockId blockId, int offset, string val, bool okToLog)
        {
            concurrencyManager.RequestExclusiveLock(blockId);
            Buffer buff = txBuffers.getBuffer(blockId);
            int lsn = -1;
            if (okToLog)
                lsn = recoveryManager.setString(buff, offset, val);
            Page p = buff.Page;
            p.SetString(offset, val);
            buff.SetModified(txNumber, lsn);
        }

        /**
         * Return the number of blocks in the specified file.
         * This method first obtains an SLock on the 
         * "end of the file", before asking the file manager
         * to return the file size.
         * @param filename the name of the file
         * @return the number of blocks in the file
         */
        public int size(string filename)
        {
            BlockId dummyblk = BlockId.Dummy(filename);
            concurrencyManager.RequestSharedLock(dummyblk);
            return fileManager.GetBlocksCount(filename);
        }

        /**
         * Append a new block to the end of the specified file
         * and returns a reference to it.
         * This method first obtains an XLock on the
         * "end of the file", before performing the append.
         * @param filename the name of the file
         * @return a reference to the newly-created disk block
         */
        public BlockId append(string filename)
        {
            BlockId dummyblk = BlockId.Dummy(filename);
            concurrencyManager.RequestExclusiveLock(dummyblk);
            return fileManager.AppendNewBlock(filename);
        }

        public int blockSize()
        {
            return fileManager.BlockSize;
        }

        public int availableBuffs()
        {
            return bufferManager.GetAvailableBufferCount();
        }

        private static int nextTxNumber()
        {
            lock(mutex)
            {
                nextTransactionNumber++;
            }

            return nextTransactionNumber;

        }
    }
}
