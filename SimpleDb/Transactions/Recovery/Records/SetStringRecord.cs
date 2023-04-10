using SimpleDb.Abstractions;
using SimpleDb.File;
using SimpleDb.Types;
using System.Buffers;

namespace SimpleDb.Transactions.Recovery.Records;

public class SetStringRecord : ILogRecord
{
    TransactionNumber transactionNumber_;
    int offset;
    DbString oldVal, newVal;
    BlockId blk;

    public LogRecordType Type => LogRecordType.SETSTRING;
    public TransactionNumber TransactionNumber => transactionNumber_;

    public SetStringRecord(Page page)
    {
        int tpos = sizeof(int);
        transactionNumber_ = page.GetTransactionNumber(tpos);
        int fpos = tpos + TransactionNumber.Size();
        DbString filename = page.GetString(fpos);
        int bpos = fpos + Page.CalculateStringStoringSize(filename);
        int blknum = page.GetInt(bpos);
        blk = BlockId.New(filename.GetString(), blknum);
        int opos = bpos + sizeof(int);
        offset = page.GetInt(opos);
        int oldvpos = opos + sizeof(int);
        oldVal = page.GetString(oldvpos);
        int newvpos = oldvpos + Page.CalculateStringStoringSize(oldVal);
        newVal = page.GetString(newvpos);
    }

    public static LSN WriteToLog(ILogManager lm, in TransactionNumber txnum, BlockId blk, int offset, DbString oldVal, DbString newVal)
    {
        int tpos = sizeof(int);
        int fpos = tpos + TransactionNumber.Size();
        int bpos = fpos + Page.CalculateStringStoringSize(blk.FileName);
        int opos = bpos + sizeof(int);
        int oldvpos = opos + sizeof(int);
        int newvpos = oldvpos + Page.CalculateStringStoringSize(oldVal);
        int reclen = newvpos + Page.CalculateStringStoringSize(newVal);

        byte[] rec = ArrayPool<byte>.Shared.Rent(reclen);
        Page p = new Page(rec);
        p.SetValue(0, (int)LogRecordType.SETSTRING);
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

    public override string ToString()
    {
        return "<SETSTRING " + transactionNumber_.Value + " " + blk + " " + offset + " " + oldVal.GetString() + " " + newVal.GetString() + ">";
    }
}
