using SimpleDB.file;
using SimpleDB.log;
using SimpleDB.Tx;
using SimpleDB.Tx.Recovery;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.Tx.Recovery
{
    internal class CheckpointRecord : LogRecord
    {
        public CheckpointRecord()
        {
        }

        public LogRecord.Type op()
        {
            return LogRecord.Type.CHECKPOINT;
        }

        /**
         * Checkpoint records have no associated transaction,
         * and so the method returns a "dummy", negative txid.
         */
        public int txNumber()
        {
            return -1; // dummy value
        }

        /**
         * Does nothing, because a checkpoint record
         * contains no undo information.
         */
        public void undo(Transaction tx) { }

        public override String ToString()
        {
            return "<CHECKPOINT>";
        }

        /** 
         * A static method to write a checkpoint record to the log.
         * This log record contains the CHECKPOINT operator,
         * and nothing else.
         * @return the LSN of the last log value
         */
        public static int writeToLog(LogManager lm)
        {
            byte[] rec = ArrayPool<byte>.Shared.Rent(sizeof(int));
            Page p = new Page(rec);
            p.SetInt(0, (int)LogRecord.Type.CHECKPOINT);
            var lsn = lm.Append(rec);
            ArrayPool<byte>.Shared.Return(rec);
            return lsn;
        }

        public void apply(Transaction tx)
        {
        }
    }
}
