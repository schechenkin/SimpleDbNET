using System.Diagnostics;
using SimpleDb.Abstractions;
using SimpleDb.Buffers;
using SimpleDb.File;
using SimpleDb.Log;
using SimpleDb.Transactions.Recovery.Records;
using SimpleDb.Types;

namespace SimpleDb.Transactions.Recovery;

public class RecoveryManager : IRecoveryManager
{
    private ILogManager logManager;
    private BufferManager bufferManager;
    private readonly LogWriteMode logWriteMode;
    private Transaction transaction;
    private TransactionNumber txnum;

    /**
     * Create a recovery manager for the specified transaction.
     * @param txnum the ID of the specified transaction
     */
    public RecoveryManager(Transaction tx, ILogManager lm, BufferManager bm, LogWriteMode logWriteMode = LogWriteMode.Sync)
    {
        this.transaction = tx;
        this.txnum = tx.Number;
        this.logManager = lm;
        this.bufferManager = bm;
        this.logWriteMode = logWriteMode;
        if (!tx.IsReadOnly)
        {
            StartRecord.WriteToLog(lm, txnum);
        }
    }

    /**
     * Write a commit record to the log, and flushes it to disk.
     */
    public void Commit()
    {
        if(!transaction.IsReadOnly)
        {
            var lsn = CommitRecord.WriteToLog(logManager, txnum);
            if(logWriteMode == LogWriteMode.Sync)
                logManager.Flush(lsn);
        }
    }

    /**
     * Write a rollback record to the log and flush it to disk.
     */
    public void Rollback()
    {
        doRollback();
        bufferManager.FlushAll(txnum);
        var lsn = RollbackRecord.WriteToLog(logManager, txnum);
        if (logWriteMode == LogWriteMode.Sync)
            logManager.Flush(lsn);
    }

    /**
     * Recover uncompleted transactions from the log
     * and then write a quiescent checkpoint record to the log and flush it.
     */
    public void Recover()
    {
        var commitedTxs = RevertNotFinishedTransactionsChanges();
        ApplyAllTransactionsFromBeginning(commitedTxs);

        bufferManager.FlushAll(txnum);
        var lsn = CheckpointRecord.WriteToLog(logManager);
        if (logWriteMode == LogWriteMode.Sync)
            logManager.Flush(lsn);
    }

    internal LSN SetValue<T>(Buffers.Buffer buffer, int offset, T value)
    {
        Debug.Assert(buffer.BlockId.HasValue);
        BlockId blk = buffer.BlockId.Value;

        switch(value)
        {
            case int intVal when value is int:
                int intValOld = buffer.Page.GetInt(offset);
                return SetIntRecord.WriteToLog(logManager, txnum, blk, offset, intValOld, intVal);

            case DbString str when value is DbString:
                DbString strOld = buffer.Page.GetString(offset);
                return SetStringRecord.WriteToLog(logManager, txnum, blk, offset, strOld, str);

            case DateTime dt when value is DateTime:
                long dtOld = buffer.Page.GetLong(offset);
                return SetDateTimeRecord.WriteToLog(logManager, txnum, blk, offset, dtOld, dt.Ticks);

            default:
                throw new NotImplementedException();
        }
    }

    internal LSN SetBit(Buffers.Buffer buff, int offset, int bitLocation, bool value)
    {
        Debug.Assert(buff.BlockId.HasValue);

        bool oldval = buff.Page.GetBit(offset, bitLocation);
        BlockId blk = buff.BlockId.Value;
        return SetNullRecord.WriteToLog(logManager, txnum, blk, offset, bitLocation, oldval, value);
    }

    /**
     * Rollback the transaction, by iterating
     * through the log records until it finds 
     * the transaction's START record,
     * calling undo() for each of the transaction's
     * log records.
     */
    private void doRollback()
    {
        var iter = logManager.GetReverseIterator();
        while (iter.HasNext())
        {
            byte[] bytes = iter.Next();
            ILogRecord rec = LogRecordFactory.CreateLogRecord(bytes);
            if (rec.TransactionNumber == txnum)
            {
                if (rec.Type == LogRecordType.START)
                    return;
                rec.Undo(transaction);
            }
        }
    }

    /**
     * Whenever it finds a log record for an unfinished
     * transaction, it calls undo() on that record.
     * The method stops when it encounters a CHECKPOINT record
     * or the end of the log.
     */
    private List<TransactionNumber> RevertNotFinishedTransactionsChanges()
    {
        var finishedTxs = new List<TransactionNumber>();
        var commitedTxs = new List<TransactionNumber>();
        var iter = logManager.GetReverseIterator();
        while (iter.HasNext())
        {
            byte[] bytes = iter.Next();
            ILogRecord rec = LogRecordFactory.CreateLogRecord(bytes);
            if (rec.Type == LogRecordType.CHECKPOINT)
                return commitedTxs;
            if (rec.Type == LogRecordType.COMMIT || rec.Type == LogRecordType.ROLLBACK)
            {
                finishedTxs.Add(rec.TransactionNumber);
                if (rec.Type == LogRecordType.COMMIT)
                {
                    commitedTxs.Add(rec.TransactionNumber);
                }
            }
            else if (!finishedTxs.Contains(rec.TransactionNumber))
                rec.Undo(transaction);
        }

        return commitedTxs;
    }

    private void ApplyAllTransactionsFromBeginning(List<TransactionNumber> commitedTxs)
    {
        var iter = logManager.GetIterator();
        while (iter.HasNext())
        {
            byte[] bytes = iter.Next();
            ILogRecord rec = LogRecordFactory.CreateLogRecord(bytes);
            var txNum = rec.TransactionNumber;
            if (commitedTxs.Contains(txNum))
            {
                rec.Apply(transaction);
            }
        }
    }
}
