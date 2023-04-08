using SimpleDb.Abstractions;
using SimpleDb.File;
using System.Buffers;

namespace SimpleDb.Transactions.Recovery.Records;

public class CheckpointRecord : ILogRecord
{
    public LogRecordType Type => LogRecordType.CHECKPOINT;
    public TransactionNumber TransactionNumber => -1;

    public static LSN WriteToLog(ILogManager lm)
    {
        byte[] rec = ArrayPool<byte>.Shared.Rent(sizeof(int));
        Page p = new Page(rec);
        p.SetInt(0, (int)LogRecordType.CHECKPOINT);
        var lsn = lm.Append(rec);
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
        return "<CHECKPOINT>";
    }

}
