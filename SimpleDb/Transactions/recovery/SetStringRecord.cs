using SimpleDB.file;
using SimpleDB.log;
using System.Buffers;

namespace SimpleDB.Tx.Recovery
{
    internal class SetStringRecord : LogRecord
    {
        private int txnum, offset;
        private string oldVal, newVal;
        private BlockId blk;

        /**
         * Create a new setint log record.
         * @param bb the bytebuffer containing the log values
         */
        public SetStringRecord(Page p)
        {
            int tpos = sizeof(int);
            txnum = p.GetInt(tpos);
            int fpos = tpos + sizeof(int);
            string filename = p.GetString(fpos);
            int bpos = fpos + Page.CalculateStringStoringSize(filename);
            int blknum = p.GetInt(bpos);
            blk = BlockId.New(filename, blknum);
            int opos = bpos + sizeof(int);
            offset = p.GetInt(opos);
            int oldvpos = opos + sizeof(int);
            oldVal = p.GetString(oldvpos);
            int newvpos = oldvpos + Page.CalculateStringStoringSize(oldVal);
            newVal = p.GetString(newvpos);
        }

        public LogRecord.Type op()
        {
            return LogRecord.Type.SETSTRING;
        }

        public int txNumber()
        {
            return txnum;
        }

        public override string ToString()
        {
            return "<SETSTRING " + txnum + " " + blk + " " + offset + " " + oldVal + " " + newVal + ">";
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
            tx.SetString(blk, offset, oldVal, false); // don't log the undo!
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
        public static int writeToLog(LogManager lm, int txnum, BlockId blk, int offset, string oldVal, string newVal)
        {
            int tpos = sizeof(int);
            int fpos = tpos + sizeof(int);
            int bpos = fpos + Page.CalculateStringStoringSize(blk.FileName);
            int opos = bpos + sizeof(int);
            int oldvpos = opos + sizeof(int);
            int newvpos = oldvpos + Page.CalculateStringStoringSize(oldVal);
            int reclen = newvpos + Page.CalculateStringStoringSize(newVal);

            byte[] rec = ArrayPool<byte>.Shared.Rent(reclen);
            Page p = new Page(rec);
            p.SetInt(0, (int)LogRecord.Type.SETSTRING);
            p.SetInt(tpos, txnum);
            p.SetString(fpos, blk.FileName);
            p.SetInt(bpos, (int)blk.Number);
            p.SetInt(opos, offset);
            p.SetString(oldvpos, oldVal);
            p.SetString(newvpos, newVal);

            var lsn = lm.Append(rec);
            ArrayPool<byte>.Shared.Return(rec);
            return lsn;
        }

        public void apply(Transaction tx)
        {
            tx.PinBlock(blk);
            tx.SetString(blk, offset, newVal, false);
            tx.UnpinBlock(blk);
        }
    }
}
