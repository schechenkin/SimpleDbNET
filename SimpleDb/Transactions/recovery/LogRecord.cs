using SimpleDB.file;
using SimpleDB.Tx;

namespace SimpleDB.Tx.Recovery
{
    internal interface LogRecord
    {
        /**
         * Returns the log record's type. 
         * @return the log record's type
         */
        LogRecord.Type op();

        /**
         * Returns the transaction id stored with
         * the log record.
         * @return the log record's transaction id
         */
        int txNumber();

        /**
         * Undoes the operation encoded by this log record.
         * The only log record types for which this method
         * does anything interesting are SETINT and SETSTRING.
         * @param txnum the id of the transaction that is performing the undo.
         */
        void undo(Transaction tx);

        void apply(Transaction tx);

        public enum Type
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
        
        public static LogRecord createLogRecord(byte[] bytes)
        {
            Page p = new Page(bytes);
            switch ((LogRecord.Type)p.GetInt(0))
            {
                case Type.CHECKPOINT:
                    return new CheckpointRecord();
                case Type.START:
                    return new StartRecord(p);
                case Type.COMMIT:
                    return new CommitRecord(p);
                case Type.ROLLBACK:
                    return new RollbackRecord(p);
                case Type.SETINT:
                    return new SetIntRecord(p);
                case Type.SETSTRING:
                    return new SetStringRecord(p);
                case Type.SETNULL:
                    return new SetNullRecord(p);
                case Type.SETDATETIME:
                    return new SetDateTimeRecord(p);
                default:
                    return null;
            }
        }
    }
}
