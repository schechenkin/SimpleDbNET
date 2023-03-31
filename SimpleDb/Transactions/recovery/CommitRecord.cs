using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Tx;
using SimpleDB.Tx.Recovery;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.Tx.Recovery
{
    internal class CommitRecord : LogRecord
    {
        private int txnum;

        public CommitRecord(Page p)
        {
            int tpos = sizeof(int);
            txnum = p.GetInt(tpos);
        }

        public LogRecord.Type op()
        {
            return LogRecord.Type.COMMIT;
        }

        public int txNumber()
        {
            return txnum;
        }

        /**
         * Does nothing, because a commit record
         * contains no undo information.
         */
        public void undo(Transaction tx) { }

        public override String ToString()
        {
            return "<COMMIT " + txnum + ">";
        }

        /** 
         * A static method to write a commit record to the log.
         * This log record contains the COMMIT operator,
         * followed by the transaction id.
         * @return the LSN of the last log value
         */
        public static int writeToLog(LogManager lm, int txnum)
        {
            byte[] rec = ArrayPool<byte>.Shared.Rent(2 * sizeof(int));
            Page p = new Page(rec);
            p.SetInt(0, (int)LogRecord.Type.COMMIT);
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
