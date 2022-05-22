using SimpleDB.file;
using SimpleDB.log;

namespace SimpleDB.Tx.Recovery
{
    internal class SetStringRecord : LogRecord
    {
        private int txnum, offset;
        private string val;
        private BlockId blk;

        /**
         * Create a new setint log record.
         * @param bb the bytebuffer containing the log values
         */
        public SetStringRecord(Page p)
        {
            int tpos = sizeof(int);
            txnum = p.getInt(tpos);
            int fpos = tpos + sizeof(int);
            string filename = p.getString(fpos);
            int bpos = fpos + Page.maxLength(filename.Length);
            int blknum = p.getInt(bpos);
            blk = new BlockId(filename, blknum);
            int opos = bpos + sizeof(int);
            offset = p.getInt(opos);
            int vpos = opos + sizeof(int);
            val = p.getString(vpos);
        }

        public LogRecord.Type op()
        {
            return LogRecord.Type.SETSTRING;
        }

        public int txNumber()
        {
            return txnum;
        }

        public string toString()
        {
            return "<SETSTRING " + txnum + " " + blk + " " + offset + " " + val + ">";
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
            tx.pin(blk);
            tx.setString(blk, offset, val, false); // don't log the undo!
            tx.unpin(blk);
        }

        /**
         * A static method to write a setInt record to the log.
         * This log record contains the SETINT operator,
         * followed by the transaction id, the filename, number,
         * and offset of the modified block, and the previous
         * integer value at that offset.
         * @return the LSN of the last log value
         */
        public static int writeToLog(LogMgr lm, int txnum, BlockId blk, int offset, string val)
        {
            int tpos = sizeof(int);
            int fpos = tpos + sizeof(int);
            int bpos = fpos + Page.maxLength(blk.fileName().Length);
            int opos = bpos + sizeof(int);
            int vpos = opos + sizeof(int);
            int reclen = vpos + Page.maxLength(val.Length);
            byte[] rec = new byte[reclen];
            Page p = new Page(rec);
            p.setInt(0, (int)LogRecord.Type.SETSTRING);
            p.setInt(tpos, txnum);
            p.setString(fpos, blk.fileName());
            p.setInt(bpos, blk.number());
            p.setInt(opos, offset);
            p.setString(vpos, val);
            return lm.append(rec);
        }
    }
}
