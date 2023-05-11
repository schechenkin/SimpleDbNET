using SimpleDb.Abstractions;
using SimpleDb.File;
using SimpleDb.Record;
using SimpleDb.Types;
using System.Buffers;

namespace SimpleDb.Transactions.Recovery.Records;

public class SetIntRecord : ILogRecord
{
    TransactionNumber transactionNumber_;
    int offset, oldVal, newVal;
    BlockId blk;

    public LogRecordType Type => LogRecordType.SETINT;
    public TransactionNumber TransactionNumber => transactionNumber_;

    public SetIntRecord(Page page)
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
        oldVal = page.GetInt(oldvpos);
        int newvpos = oldvpos + sizeof(int);
        newVal = page.GetInt(newvpos);
    }

    public static LSN WriteToLog(ILogManager logManager, in TransactionNumber txnum, BlockId blk, int offset, int oldVal, int newVal)
    {
        int tpos = sizeof(int);
        int fpos = tpos + TransactionNumber.Size();
        int bpos = fpos + Page.CalculateStringStoringSize(blk.FileName);
        int opos = bpos + sizeof(int);
        int oldvpos = opos + sizeof(int);
        int newvpos = oldvpos + sizeof(int);
        byte[] rec = ArrayPool<byte>.Shared.Rent(newvpos + sizeof(int));
        Page page = new Page(rec);
        page.SetValue(0, (int)LogRecordType.SETINT);
        page.SetValue(tpos, txnum);
        page.SetValue(fpos, blk.FileName);
        page.SetValue(bpos, (int)blk.Number);
        page.SetValue(opos, offset);
        page.SetValue(oldvpos, oldVal);
        page.SetValue(newvpos, newVal);

        var lsn = logManager.Append(rec);
        ArrayPool<byte>.Shared.Return(rec);
        return lsn;
    }

    public void Apply(Transaction tx)
    {
        tx.PinBlock(blk);
        if(tx.GetBuffer(blk).Page.GetTransactionNumber(RecordPage.TransactionNumberOffset) <  transactionNumber_)
        {
            tx.SetValue(blk, offset, newVal, false);
        }
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
        return $"<SETINT {transactionNumber_.Value} {blk} {offset} {oldVal} {newVal}>";
    }
}
