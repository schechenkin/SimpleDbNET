using SimpleDb.Abstractions;
using SimpleDb.File;
using System.Buffers;

namespace SimpleDb.Transactions.Recovery.Records;

public class RollbackRecord : ILogRecord
{
    private TransactionNumber transactionNumber_;

    public LogRecordType Type => LogRecordType.COMMIT;
    public TransactionNumber TransactionNumber => transactionNumber_;

    public RollbackRecord(Page p)
    {
        int tpos = sizeof(int);
        transactionNumber_ = p.GetTransactionNumber(tpos);
    }

    public static LSN WriteToLog(ILogManager logManager, in TransactionNumber transactionNumber)
    {
        byte[] rec = ArrayPool<byte>.Shared.Rent(sizeof(int) + SimpleDb.Abstractions.TransactionNumber.Size());
        Page p = new Page(rec);
        p.SetInt(0, (int)LogRecordType.ROLLBACK);
        p.SetValue(sizeof(int), transactionNumber);
        var lsn = logManager.Append(rec);
        ArrayPool<byte>.Shared.Return(rec);
        return lsn;
    }

    public void Apply(Transaction tx)
    {
    }

    public void Undo(Transaction tx)
    {
    }

    public override String ToString()
    {
        return "<ROLLBACK " + transactionNumber_.Value + ">";
    }
}
