using SimpleDB.file;
using SimpleDB.log;

namespace SimpleDB.Tx.Recovery
{
    internal class SetNullRecord : LogRecord
    {
        private int txnum;

        /**
         * Create a new setint log record.
         * @param bb the bytebuffer containing the log values
         */
        public SetNullRecord(Page p)
        {
            int tpos = sizeof(int);
            txnum = p.GetInt(tpos);
        }

        public LogRecord.Type op()
        {
            return LogRecord.Type.SETNULL;
        }

        public int txNumber()
        {
            return txnum;
        }

        public string toString()
        {
            return "<SETNULL " + txnum + ">";
        }

        /**
         * Replace the specified data value with the value saved in the log record.
         * The method pins a buffer to the specified block,
         * calls setInt to restore the saved value,
         * and unpins the buffer.
         * @see simpledb.tx.recovery.LogRecord#undo(int)
         */
        public void undo(Transaction tx)
        {
            throw new NotImplementedException();
        }

        /**
         * A static method to write a setInt record to the log.
         * This log record contains the SETINT operator,
         * followed by the transaction id, the filename, number,
         * and offset of the modified block, and the previous
         * integer value at that offset.
         * @return the LSN of the last log value
         */
        public static int writeToLog(LogManager lm, int txnum, BlockId blk, int offset, string val)
        {
            int tpos = sizeof(int);
            int fpos = tpos + sizeof(int);
            int bpos = fpos + Page.CalculateStringStoringSize(blk.FileName);
            int opos = bpos + sizeof(int);
            int vpos = opos + sizeof(int);
            int reclen = vpos + Page.CalculateStringStoringSize(val);
            byte[] rec = new byte[reclen];
            Page p = new Page(rec);
            p.SetInt(0, (int)LogRecord.Type.SETNULL);
            return lm.Append(rec);
        }
    }
}
