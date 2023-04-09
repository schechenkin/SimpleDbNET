using SimpleDb.Abstractions;
using SimpleDb.Buffers;
using SimpleDb.File;
using SimpleDb.Transactions.Concurrency;
using SimpleDb.Transactions.Recovery;
using SimpleDb.Types;

namespace SimpleDb.Transactions;

public class Transaction
{
    private static int nextTransactionNumber = 0;
    private static int END_OF_FILE = -1;

    private RecoveryManager recoveryManager;
    private ConcurrencyManager concurrencyManager;
    private BufferManager bufferManager;
    private IFileManager fileManager;
    private TransactionNumber txNumber;
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
    public Transaction(IFileManager fileManager, ILogManager logManager, BufferManager bufferManager, LockTable lockTable)
    {
        this.fileManager = fileManager;
        this.bufferManager = bufferManager;
        txNumber = nextTxNumber();
        recoveryManager = new RecoveryManager(this, txNumber, logManager, bufferManager);
        concurrencyManager = new ConcurrencyManager(lockTable);
        txBuffers = new BufferList(bufferManager);
    }

    public TransactionNumber Number => txNumber;

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
    public void PinBlock(in BlockId blockId)
    {
        txBuffers.pin(blockId);
    }

    /**
     * Unpin the specified block.
     * The transaction looks up the buffer pinned to this block,
     * and unpins it.
     * @param blk a reference to the disk block
     */
    public void UnpinBlock(in BlockId blockId)
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
    public int GetInt(in BlockId blockId, int offset)
    {
        concurrencyManager.RequestSharedLock(blockId);
        var buff = txBuffers.getBuffer(blockId);
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
    public DbString GetString(in BlockId blockId, int offset)
    {
        concurrencyManager.RequestSharedLock(blockId);
        var buffer = txBuffers.getBuffer(blockId);
        return buffer.Page.GetString(offset);
    }

    public DateTime GetDateTime(in BlockId blockId, int offset)
    {
        concurrencyManager.RequestSharedLock(blockId);
        var buffer = txBuffers.getBuffer(blockId);
        return buffer.Page.GetDateTime(offset);
    }

    /*public bool CompareString(in BlockId blockId, int offset, StringConstant val)
    {
        concurrencyManager.RequestSharedLock(blockId);
        var buffer = txBuffers.getBuffer(blockId);
        return buffer.Page.StringCompare(offset, val);
    }*/

    public bool GetBitValue(in BlockId blockId, int offset, int bitLocation)
    {
        concurrencyManager.RequestSharedLock(blockId);
        var buffer = txBuffers.getBuffer(blockId);
        return buffer.Page.GetBit(offset, bitLocation);
    }

    public void SetValue<T>(in BlockId blockId, int offset, T value, bool writeToLog)
    {
        concurrencyManager.RequestExclusiveLock(blockId);
        var buffer = txBuffers.getBuffer(blockId);
        LSN lsn = -1;
        if (writeToLog)
            lsn = recoveryManager.SetValue(buffer, offset, value);
        Page p = buffer.Page;
        p.SetValue(offset, value);
        buffer.SetModified(txNumber, lsn);
    }

    public void SetBit(in BlockId blockId, int offset, int bitLocation, bool value, bool okToLog)
    {
        concurrencyManager.RequestExclusiveLock(blockId);
        var buff = txBuffers.getBuffer(blockId);
        LSN lsn = -1;
        if (okToLog)
            lsn = recoveryManager.SetBit(buff, offset, bitLocation, value);
        Page p = buff.Page;
        p.SetBit(offset, bitLocation, value);
        buff.SetModified(txNumber, lsn);
    }

    /*public void SetValue(in BlockId blockId, int offset, string value, bool writeToLog)
    {
        SetValue(blockId, offset, (DbString)value, writeToLog);
    }*/

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

    private static int nextTxNumber()
    {
        lock (mutex)
        {
            nextTransactionNumber++;
        }

        return nextTransactionNumber;

    }

    public SimpleDb.Buffers.Buffer GetBuffer(in BlockId blockId)
    {
        return txBuffers.getBuffer(blockId);
    }
}