using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.Tx.Recovery
{
    public class RecoveryMgr : IRecoveryManager
    {
        private LogManager logManager;
        private BufferManager bufferManager;
        private Transaction tx;
        private int txnum;

        /**
         * Create a recovery manager for the specified transaction.
         * @param txnum the ID of the specified transaction
         */
        public RecoveryMgr(Transaction tx, int txnum, LogManager lm, BufferManager bm)
        {
            this.tx = tx;
            this.txnum = txnum;
            this.logManager = lm;
            this.bufferManager = bm;
            StartRecord.writeToLog(lm, txnum);
        }

        /**
         * Write a commit record to the log, and flushes it to disk.
         */
        public void commit()
        {
            //bufferManager.FlushAll(txnum);
            int lsn = CommitRecord.writeToLog(logManager, txnum);
            logManager.Flush(lsn);
        }

        /**
         * Write a rollback record to the log and flush it to disk.
         */
        public void rollback()
        {
            doRollback();
            bufferManager.FlushAll(txnum);
            int lsn = RollbackRecord.writeToLog(logManager, txnum);
            logManager.Flush(lsn);
        }

        /**
         * Recover uncompleted transactions from the log
         * and then write a quiescent checkpoint record to the log and flush it.
         */
        public void recover()
        {
            var commitedTxs = RevertNotFinishedTransactionsChanges();
            ApplyAllTransactionsFromBeginning(commitedTxs);

            bufferManager.FlushAll(txnum);
            int lsn = CheckpointRecord.writeToLog(logManager);
            logManager.Flush(lsn);
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
            return SetIntRecord.writeToLog(logManager, txnum, blk, offset, oldval, newval);
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
            return SetStringRecord.writeToLog(logManager, txnum, blk, offset, oldval, newval);
        }

        public int setDateTime(Data.Buffer buff, int offset, DateTime dateTime)
        {
            DateTime oldval = buff.Page.GetDateTime(offset);
            BlockId blk = buff.BlockId.Value;
            return SetDateTimeRecord.writeToLog(logManager, txnum, blk, offset, oldval, dateTime);
        }

        internal int SetBit(Data.Buffer buff, int offset, int bitLocation, bool value)
        {
            string oldval = buff.Page.GetString(offset);
            BlockId blk = buff.BlockId.Value;
            return SetNullRecord.writeToLog(logManager, txnum, blk, offset, oldval);
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
            var iter = logManager.GetIterator();
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
         * Whenever it finds a log record for an unfinished
         * transaction, it calls undo() on that record.
         * The method stops when it encounters a CHECKPOINT record
         * or the end of the log.
         */
        private List<int> RevertNotFinishedTransactionsChanges()
        {
            var finishedTxs = new List<int>();
            var commitedTxs = new List<int>();
            var iter = logManager.GetIterator();
            while (iter.HasNext())
            {
                byte[] bytes = iter.Next();
                LogRecord rec = LogRecord.createLogRecord(bytes);
                if (rec.op() == LogRecord.Type.CHECKPOINT)
                    return commitedTxs;
                if (rec.op() == LogRecord.Type.COMMIT || rec.op() == LogRecord.Type.ROLLBACK)
                {
                    finishedTxs.Add(rec.txNumber());
                    if(rec.op() == LogRecord.Type.COMMIT)
                    {
                        commitedTxs.Add(rec.txNumber());
                    }
                }
                else if (!finishedTxs.Contains(rec.txNumber()))
                    rec.undo(tx);
            }

            return commitedTxs;
        }

        private void ApplyAllTransactionsFromBeginning(List<int> commitedTxs)
        {
            var iter = logManager.GetReverseIterator();
            while (iter.HasNext())
            {
                byte[] bytes = iter.Next();
                LogRecord rec = LogRecord.createLogRecord(bytes);
                var txNum = rec.txNumber();
                if(commitedTxs.Contains(txNum))
                {
                    rec.apply(tx);
                }
            }
        }

    }
}
