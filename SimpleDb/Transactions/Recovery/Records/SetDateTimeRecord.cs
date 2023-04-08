using SimpleDb.Abstractions;
using SimpleDb.File;
using System.Buffers;

namespace SimpleDb.Transactions.Recovery.Records;

public class SetDateTimeRecord : ILogRecord
{
    TransactionNumber transactionNumber_;
    private int offset;
    private long oldVal, newVal;
    private BlockId blk;

    public LogRecordType Type => LogRecordType.SETDATETIME;
    public TransactionNumber TransactionNumber => transactionNumber_;

    public SetDateTimeRecord(Page p)
    {
        int tpos = sizeof(int);
        transactionNumber_ = p.GetTransactionNumber(tpos);
        int fpos = tpos + TransactionNumber.Size();
        String filename = p.GetString(fpos);
        int bpos = fpos + Page.CalculateStringStoringSize(filename);
        int blknum = p.GetInt(bpos);
        blk = BlockId.New(filename, blknum);
        int opos = bpos + sizeof(int);
        offset = p.GetInt(opos);
        int oldvpos = opos + sizeof(int);
        oldVal = p.GetLong(oldvpos);
        int newvpos = oldvpos + sizeof(long);
        newVal = p.GetLong(newvpos);
    }

    public static LSN WriteToLog(ILogManager lm, in TransactionNumber txnum, BlockId blk, int offset, long oldVal, long newVal)
    {
        int tpos = sizeof(int);
        int fpos = tpos + TransactionNumber.Size();
        int bpos = fpos + Page.CalculateStringStoringSize(blk.FileName);
        int opos = bpos + sizeof(int);
        int oldvpos = opos + sizeof(int);
        int newvpos = oldvpos + sizeof(long);
        byte[] rec = ArrayPool<byte>.Shared.Rent(newvpos + sizeof(long));
        Page p = new Page(rec);
        p.SetValue(0, (int)LogRecordType.SETDATETIME);
        p.SetValue(tpos, txnum);
        p.SetValue(fpos, blk.FileName);
        p.SetValue(bpos, (int)blk.Number);
        p.SetValue(opos, offset);
        p.SetValue(oldvpos, oldVal);
        p.SetValue(newvpos, newVal);
        var lsn = lm.Append(rec);
        ArrayPool<byte>.Shared.Return(rec);
        return lsn;
    }


    public void Apply(Transaction tx)
    {
        tx.PinBlock(blk);
        tx.SetValue(blk, offset, newVal, false);
        tx.UnpinBlock(blk);
    }

    public void Undo(Transaction tx)
    {
        tx.PinBlock(blk);
        tx.SetValue(blk, offset, oldVal, false); // don't log the undo!
        tx.UnpinBlock(blk);
    }

    public override String ToString()
    {
        return "<SETDATETIME " + transactionNumber_.Value + " " + blk + " " + offset + " " + oldVal + " " + newVal + ">";
    }
}
