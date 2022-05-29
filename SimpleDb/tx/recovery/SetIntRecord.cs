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
            txnum = p.GetInt(tpos);
            int fpos = tpos + sizeof(int);
            String filename = p.GetString(fpos);
            int bpos = fpos + Page.CalculateStringStoringSize(filename);
            int blknum = p.GetInt(bpos);
            blk = BlockId.New(filename, blknum);
            int opos = bpos + sizeof(int);
            offset = p.GetInt(opos);
            int vpos = opos + sizeof(int);
            val = p.GetInt(vpos);
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
        public static int writeToLog(LogManager lm, int txnum, BlockId blk, int offset, int val)
        {
            int tpos = sizeof(int);
            int fpos = tpos + sizeof(int);
            int bpos = fpos + Page.CalculateStringStoringSize(blk.FileName);
            int opos = bpos + sizeof(int);
            int vpos = opos + sizeof(int);
            byte[] rec = new byte[vpos + sizeof(int)];
            Page p = new Page(rec);
            p.SetInt(0, (int)LogRecord.Type.SETINT);
            p.SetInt(tpos, txnum);
            p.SetString(fpos, blk.FileName);
            p.SetInt(bpos, (int)blk.Number);
            p.SetInt(opos, offset);
            p.SetInt(vpos, val);
            return lm.Append(rec);
        }
    }
}
