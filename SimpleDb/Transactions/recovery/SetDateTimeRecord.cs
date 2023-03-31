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
    internal class SetDateTimeRecord : LogRecord
    {
        private int txnum, offset;
        private DateTime oldVal, newVal;
        private BlockId blk;

        /**
         * Create a new setint log record.
         * @param bb the bytebuffer containing the log values
         */
        public SetDateTimeRecord(Page p)
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
            int oldvpos = opos + sizeof(int);
            oldVal = p.GetDateTime(oldvpos);
            int newvpos = oldvpos + sizeof(long);
            newVal = p.GetDateTime(newvpos);
        }

        public LogRecord.Type op()
        {
            return LogRecord.Type.SETDATETIME;
        }

        public int txNumber()
        {
            return txnum;
        }

        public override String ToString()
        {
            return "<SETDATETIME " + txnum + " " + blk + " " + offset + " " + oldVal + " " + newVal + ">";
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
            tx.PinBlock(blk);
            tx.SetDateTime(blk, offset, oldVal, false); // don't log the undo!
            tx.UnpinBlock(blk);
        }

        /**
         * A static method to write a setInt record to the log.
         * This log record contains the SETINT operator,
         * followed by the transaction id, the filename, number,
         * and offset of the modified block, and the previous
         * integer value at that offset.
         * @return the LSN of the last log value
         */
        public static int writeToLog(LogManager lm, int txnum, BlockId blk, int offset, DateTime oldVal, DateTime newVal)
        {
            int tpos = sizeof(int);
            int fpos = tpos + sizeof(int);
            int bpos = fpos + Page.CalculateStringStoringSize(blk.FileName);
            int opos = bpos + sizeof(int);
            int oldvpos = opos + sizeof(int);
            int newvpos = oldvpos + sizeof(long);
            byte[] rec = ArrayPool<byte>.Shared.Rent(newvpos + sizeof(long));
            Page p = new Page(rec);
            p.SetInt(0, (int)LogRecord.Type.SETDATETIME);
            p.SetInt(tpos, txnum);
            p.SetString(fpos, blk.FileName);
            p.SetInt(bpos, (int)blk.Number);
            p.SetInt(opos, offset);
            p.SetDateTime(oldvpos, oldVal);
            p.SetDateTime(newvpos, newVal);
            var lsn = lm.Append(rec);
            ArrayPool<byte>.Shared.Return(rec);
            return lsn;
        }

        public void apply(Transaction tx)
        {
            tx.PinBlock(blk);
            tx.SetDateTime(blk, offset, newVal, false);
            tx.UnpinBlock(blk);
        }
    }
}
