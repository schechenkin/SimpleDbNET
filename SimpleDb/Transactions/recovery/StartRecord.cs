using SimpleDB.file;
using SimpleDB.log;
using System;
using System.Buffers;

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
            txnum = p.GetInt(tpos);
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

        public override String ToString()
        {
            return "<START " + txnum + ">";
        }

        /** 
         * A static method to write a start record to the log.
         * This log record contains the START operator,
         * followed by the transaction id.
         * @return the LSN of the last log value
         */
        public static int writeToLog(LogManager lm, int txnum)
        {
            byte[] rec = ArrayPool<byte>.Shared.Rent(2 * sizeof(int));
            Page p = new Page(rec);
            p.SetInt(0, (int)LogRecord.Type.START);
            p.SetInt(sizeof(int), txnum);
            var lsn = lm.Append(rec);
            ArrayPool<byte>.Shared.Return(rec);
            return lsn;
        }

        public void apply(Transaction tx)
        {
        }
    }
}
