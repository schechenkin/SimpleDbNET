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
    internal class SetIntRecord : LogRecord
    {
        private int txnum, offset, val;
        private BlockId blk;

        /**
         * Create a new setint log record.
         * @param bb the bytebuffer containing the log values
         */
        public SetIntRecord(Page p)
        {
            int tpos = sizeof(int);
            txnum = p.getInt(tpos);
            int fpos = tpos + sizeof(int);
            String filename = p.getString(fpos);
            int bpos = fpos + Page.maxLength(filename.Length);
            int blknum = p.getInt(bpos);
            blk = new BlockId(filename, blknum);
            int opos = bpos + sizeof(int);
            offset = p.getInt(opos);
            int vpos = opos + sizeof(int);
            val = p.getInt(vpos);
        }

        public LogRecord.Type op()
        {
            return LogRecord.Type.SETINT;
        }

        public int txNumber()
        {
            return txnum;
        }

        public String toString()
        {
            return "<SETINT " + txnum + " " + blk + " " + offset + " " + val + ">";
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
            tx.setInt(blk, offset, val, false); // don't log the undo!
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
        public static int writeToLog(LogMgr lm, int txnum, BlockId blk, int offset, int val)
        {
            int tpos = sizeof(int);
            int fpos = tpos + sizeof(int);
            int bpos = fpos + Page.maxLength(blk.fileName().Length);
            int opos = bpos + sizeof(int);
            int vpos = opos + sizeof(int);
            byte[] rec = new byte[vpos + sizeof(int)];
            Page p = new Page(rec);
            p.setInt(0, (int)LogRecord.Type.SETINT);
            p.setInt(tpos, txnum);
            p.setString(fpos, blk.fileName());
            p.setInt(bpos, blk.number());
            p.setInt(opos, offset);
            p.setInt(vpos, val);
            return lm.append(rec);
        }
    }
}
