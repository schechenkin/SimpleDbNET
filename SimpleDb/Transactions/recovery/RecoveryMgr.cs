using Microsoft.Extensions.Logging;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;

namespace SimpleDB.Tx.Recovery
{
    public class RecoveryMgr : IRecoveryManager
    {
        private LogManager lm;
        private BufferManager bm;
        private Transaction tx;
        private int txnum;

        private readonly ILogger<RecoveryMgr> logger;

        /**
         * Create a recovery manager for the specified transaction.
         * @param txnum the ID of the specified transaction
         */
        public RecoveryMgr(Transaction tx, int txnum, LogManager lm, BufferManager bm, ILoggerFactory loggerFactory)
        {
            logger = loggerFactory.CreateLogger<RecoveryMgr>();
            
            this.tx = tx;
            this.txnum = txnum;
            this.lm = lm;
            this.bm = bm;
            StartRecord.writeToLog(lm, txnum);
        }

        /**
         * Write a commit record to the log, and flushes it to disk.
         */
        public void commit()
        {
            logger.LogInformation("Commit");
            
            bm.FlushAll(txnum);
            int lsn = CommitRecord.writeToLog(lm, txnum);
            lm.Flush(lsn);
        }

        /**
         * Write a rollback record to the log and flush it to disk.
         */
        public void rollback()
        {
            logger.LogInformation("Rollback");
            
            doRollback();
            bm.FlushAll(txnum);
            int lsn = RollbackRecord.writeToLog(lm, txnum);
            lm.Flush(lsn);
        }

        /**
         * Recover uncompleted transactions from the log
         * and then write a quiescent checkpoint record to the log and flush it.
         */
        public void recover()
        {
            logger.LogInformation("recover");
            
            doRecover();
            bm.FlushAll(txnum);
            int lsn = CheckpointRecord.writeToLog(lm);
            lm.Flush(lsn);
        }

        /**
         * Write a setint record to the log and return its lsn.
         * @param buff the buffer containing the page
         * @param offset the offset of the value in the page
         * @param newval the value to be written
         */
        public int setInt(Data.Buffer buff, int offset, int newval)
        {
            int oldval = buff.Page.GetInt(offset);
            BlockId blk = buff.BlockId.Value;

            logger.LogDebug("Write to log SetIntRecord");
            return SetIntRecord.writeToLog(lm, txnum, blk, offset, oldval);
        }

        /**
         * Write a setstring record to the log and return its lsn.
         * @param buff the buffer containing the page
         * @param offset the offset of the value in the page
         * @param newval the value to be written
         */
        public int setString(Data.Buffer buff, int offset, string newval)
        {
            string oldval = buff.Page.GetString(offset);
            BlockId blk = buff.BlockId.Value;

            logger.LogDebug("Write to log SetStringRecord");
            return SetStringRecord.writeToLog(lm, txnum, blk, offset, oldval);
        }

        public int setDateTime(Data.Buffer buff, int offset, DateTime dateTime)
        {
            DateTime oldval = buff.Page.GetDateTime(offset);
            BlockId blk = buff.BlockId.Value;

            logger.LogDebug("Write to log SetDateTimeRecord");
            return SetDateTimeRecord.writeToLog(lm, txnum, blk, offset, oldval);
        }

        internal int SetBit(Data.Buffer buff, int offset, int bitLocation, bool value)
        {
            string oldval = buff.Page.GetString(offset);
            BlockId blk = buff.BlockId.Value;

            logger.LogDebug("Write to log SetNullRecord");
            return SetNullRecord.writeToLog(lm, txnum, blk, offset, oldval);
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
            var iter = lm.GetIterator();
            while (iter.HasNext())
            {
                byte[] bytes = iter.Next();
                LogRecord rec = LogRecord.createLogRecord(bytes);
                if (rec.txNumber() == txnum)
                {
                    if (rec.op() == LogRecord.Type.START)
                        return;
                    rec.undo(tx);
                }
            }
        }

        /**
         * Do a complete database recovery.
         * The method iterates through the log records.
         * Whenever it finds a log record for an unfinished
         * transaction, it calls undo() on that record.
         * The method stops when it encounters a CHECKPOINT record
         * or the end of the log.
         */
        private void doRecover()
        {
            var finishedTxs = new List<int>();
            var iter = lm.GetIterator();
            while (iter.HasNext())
            {
                byte[] bytes = iter.Next();
                LogRecord rec = LogRecord.createLogRecord(bytes);
                if (rec.op() == LogRecord.Type.CHECKPOINT)
                    return;
                if (rec.op() == LogRecord.Type.COMMIT || rec.op() == LogRecord.Type.ROLLBACK)
                    finishedTxs.Add(rec.txNumber());
                else if (!finishedTxs.Contains(rec.txNumber()))
                    rec.undo(tx);
            }
        }
    }
}
