﻿using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.Tx.Recovery
{
    public class RecoveryMgr
    {
        private LogMgr lm;
        private BufferMgr bm;
        private Transaction tx;
        private int txnum;

        /**
         * Create a recovery manager for the specified transaction.
         * @param txnum the ID of the specified transaction
         */
        public RecoveryMgr(Transaction tx, int txnum, LogMgr lm, BufferMgr bm)
        {
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
            bm.flushAll(txnum);
            int lsn = CommitRecord.writeToLog(lm, txnum);
            lm.flush(lsn);
        }

        /**
         * Write a rollback record to the log and flush it to disk.
         */
        public void rollback()
        {
            doRollback();
            bm.flushAll(txnum);
            int lsn = RollbackRecord.writeToLog(lm, txnum);
            lm.flush(lsn);
        }

        /**
         * Recover uncompleted transactions from the log
         * and then write a quiescent checkpoint record to the log and flush it.
         */
        public void recover()
        {
            doRecover();
            bm.flushAll(txnum);
            int lsn = CheckpointRecord.writeToLog(lm);
            lm.flush(lsn);
        }

        /**
         * Write a setint record to the log and return its lsn.
         * @param buff the buffer containing the page
         * @param offset the offset of the value in the page
         * @param newval the value to be written
         */
        public int setInt(Data.Buffer buff, int offset, int newval)
        {
            int oldval = buff.contents().getInt(offset);
            BlockId blk = buff.block();
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
            string oldval = buff.contents().getString(offset);
            BlockId blk = buff.block();
            return SetStringRecord.writeToLog(lm, txnum, blk, offset, oldval);
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
            var iter = lm.iterator();
            while (iter.hasNext())
            {
                byte[] bytes = iter.next();
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
            var iter = lm.iterator();
            while (iter.hasNext())
            {
                byte[] bytes = iter.next();
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
