using SimpleDB.file;
using SimpleDB.log;
using System;

namespace SimpleDB.Tx.Recovery
{
    internal class StartRecord : LogRecord
    {
        private int txnum;

        /**
         * Create a log record by reading one other value from the log.
         * @param bb the bytebuffer containing the log values
         */
        public StartRecord(Page p)
        {
            int tpos = sizeof(int);
            txnum = p.getInt(tpos);
        }

        public LogRecord.Type op()
        {
            return LogRecord.Type.START;
        }

        public int txNumber()
        {
            return txnum;
        }

        /**
         * Does nothing, because a start record
         * contains no undo information.
         */
        public void undo(Transaction tx) { }

        public String toString()
        {
            return "<START " + txnum + ">";
        }

        /** 
         * A static method to write a start record to the log.
         * This log record contains the START operator,
         * followed by the transaction id.
         * @return the LSN of the last log value
         */
        public static int writeToLog(LogMgr lm, int txnum)
        {
            byte[] rec = new byte[2 * sizeof(int)];
            Page p = new Page(rec);
            p.setInt(0, (int)LogRecord.Type.START);
            p.setInt(sizeof(int), txnum);
            return lm.append(rec);
        }
    }
}
