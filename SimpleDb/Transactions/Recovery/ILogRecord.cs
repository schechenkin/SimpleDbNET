using SimpleDb.Abstractions;

namespace SimpleDb.Transactions.Recovery;

public interface ILogRecord
{
    LogRecordType Type { get; }
    TransactionNumber TransactionNumber { get; }

    /**
     * Undoes the operation encoded by this log record.
     * The only log record types for which this method
     * does anything interesting are SETINT and SETSTRING.
     * @param txnum the id of the transaction that is performing the undo.
     */
    void Undo(Transaction tx);

    void Apply(Transaction tx);

    //static abstract LSN WriteToLog(ILogManager logManager, in TransactionNumber transactionNumber);
}

public enum LogRecordType
{
    CHECKPOINT = 0,
    START = 1,
    COMMIT = 2,
    ROLLBACK = 3,
    SETINT = 4,
    SETSTRING = 5,
    SETNULL = 6,
    SETDATETIME = 7,
}
