using SimpleDb.File;
using SimpleDb.Transactions.Recovery.Records;

namespace SimpleDb.Transactions.Recovery;

public static class LogRecordFactory
{
    public static ILogRecord CreateLogRecord(byte[] bytes)
    {
        Page p = new Page(bytes);
        switch ((LogRecordType)p.GetInt(0))
        {
            case LogRecordType.CHECKPOINT:
                return new CheckpointRecord();
            case LogRecordType.START:
                return new StartRecord(p);
            case LogRecordType.COMMIT:
                return new CommitRecord(p);
            case LogRecordType.ROLLBACK:
                return new RollbackRecord(p);
            case LogRecordType.SETINT:
                return new SetIntRecord(p);
            case LogRecordType.SETSTRING:
                return new SetStringRecord(p);
            case LogRecordType.SETNULL:
                return new SetNullRecord(p);
            case LogRecordType.SETDATETIME:
                return new SetDateTimeRecord(p);
            default:
                throw new NotImplementedException();
        }
    }
}
