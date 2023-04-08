using SimpleDb.Abstractions;
using SimpleDb.File;
using System.Buffers;

namespace SimpleDb.Transactions.Recovery.Records;

public class StartRecord : ILogRecord
{
    TransactionNumber transactionNumber_;

    public LogRecordType Type => LogRecordType.START;
    public TransactionNumber TransactionNumber => transactionNumber_;

    public static LSN WriteToLog(ILogManager logManager, in TransactionNumber transactionNumber)
    {
        byte[] rec = ArrayPool<byte>.Shared.Rent(sizeof(int) + SimpleDb.Abstractions.TransactionNumber.Size());
        Page p = new Page(rec);
        p.SetInt(0, (int)LogRecordType.START);
        p.SetValue(sizeof(int), transactionNumber);
        var lsn = logManager.Append(rec);
        ArrayPool<byte>.Shared.Return(rec);
        return lsn;
    }

    public StartRecord(in Page page)
    {
        int tpos = sizeof(int);
        transactionNumber_ = page.GetTransactionNumber(tpos);
    }

    public void Apply(Transaction tx)
    {
    }

    public void Undo(Transaction tx)
    {
    }

    public override String ToString()
    {
        return "<START " + transactionNumber_.Value + ">";
    }
}
