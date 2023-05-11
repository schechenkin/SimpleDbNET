using SimpleDb.Abstractions;
using SimpleDb.File;
using SimpleDb.Record;
using SimpleDb.Types;
using System.Buffers;

namespace SimpleDb.Transactions.Recovery.Records;

public class SetNullRecord : ILogRecord
{
    TransactionNumber transactionNumber_;
    int offset, bitLocation;
    bool oldVal, newVal;
    BlockId blk;

    public LogRecordType Type => LogRecordType.SETNULL;
    public TransactionNumber TransactionNumber => transactionNumber_;

    public SetNullRecord(Page page)
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
        int blpos = opos + sizeof(int);
        bitLocation = page.GetInt(blpos);
        int oldvpos = blpos + sizeof(int);
        oldVal = page.GetBool(oldvpos);
        int newvpos = oldvpos + sizeof(bool);
        newVal = page.GetBool(newvpos);
    }

    public static LSN WriteToLog(ILogManager lm, in TransactionNumber txnum, BlockId blk, int offset, int bitLocation, bool oldval, bool newVal)
    {
        int tpos = sizeof(int);
        int fpos = tpos + TransactionNumber.Size();
        int bpos = fpos + Page.CalculateStringStoringSize(blk.FileName);
        int opos = bpos + sizeof(int);
        int blpos = opos + sizeof(int);
        int oldvpos = blpos + sizeof(int);
        int newvpos = oldvpos + sizeof(bool);
        int reclen = newvpos + sizeof(bool);
        byte[] rec = ArrayPool<byte>.Shared.Rent(reclen);
        Page page = new Page(rec);
        page.SetInt(0, (int)LogRecordType.SETNULL);
        page.SetValue(tpos, txnum);
        page.SetValue(fpos, blk.FileName);
        page.SetValue(bpos, (int)blk.Number);
        page.SetValue(opos, offset);
        page.SetValue(blpos, bitLocation);
        page.SetValue(oldvpos, oldval);
        page.SetValue(newvpos, newVal);

        var lsn = lm.Append(rec);
        ArrayPool<byte>.Shared.Return(rec);
        return lsn;
    }

    public void Apply(Transaction tx)
    {
        tx.PinBlock(blk);
        if(tx.GetBuffer(blk).Page.GetTransactionNumber(RecordPage.TransactionNumberOffset) <  transactionNumber_)
        {
            tx.SetBit(blk, offset, bitLocation, newVal, false);
        }
        tx.UnpinBlock(blk);
    }

    public void Undo(Transaction tx)
    {
        tx.PinBlock(blk);
        tx.SetBit(blk, offset, bitLocation, oldVal, false); // don't log the undo!
        tx.UnpinBlock(blk);
    }

    public override String ToString()
    {
        return $"<SETNULL {transactionNumber_.Value} {blk} {offset} {bitLocation} {oldVal} {newVal}>";
    }
}
