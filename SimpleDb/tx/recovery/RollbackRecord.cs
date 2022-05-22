using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Tx;
using SimpleDB.Tx.Recovery;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.Tx.Recovery
{
    internal class RollbackRecord : LogRecord
    {
        private int txnum;

        /**
         * Create a RollbackRecord object.
         * @param txnum the ID of the specified transaction
         */
        public RollbackRecord(Page p)
        {
            int tpos = sizeof(int);
            txnum = p.getInt(tpos);
        }

        public LogRecord.Type op()
        {
            return LogRecord.Type.ROLLBACK;
        }

        public int txNumber()
        {
            return txnum;
        }

        /**
         * Does nothing, because a rollback record
         * contains no undo information.
         */
        public void undo(Transaction tx) { }

        public String toString()
        {
            return "<ROLLBACK " + txnum + ">";
        }

        /** 
         * A static method to write a rollback record to the log.
         * This log record contains the ROLLBACK operator,
         * followed by the transaction id.
         * @return the LSN of the last log value
         */
        public static int writeToLog(LogMgr lm, int txnum)
        {
            byte[] rec = new byte[2 * sizeof(int)];
            Page p = new Page(rec);
            p.setInt(0, (int)LogRecord.Type.ROLLBACK);
            p.setInt(sizeof(int), txnum);
            return lm.append(rec);
        }
    }
}
