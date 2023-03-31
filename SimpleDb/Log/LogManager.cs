using SimpleDB.file;
using SimpleDB.Tx.Recovery;

namespace SimpleDB.log
{
    public class LogManager
    {
        private readonly FileManager m_FileManager;
        private readonly string m_Logfile;
        private Page m_LogPage;
        private BlockId m_CurrentBlockId;
        private int m_LatestLSN = 0;
        private int m_LastSavedLSN = 0;
        private object logManagerLock = new object();

        /**
         * Creates the manager for the specified log file.
         * If the log file does not yet exist, it is created
         * with an empty first block.
         * @param FileMgr the file manager
         * @param logfile the name of the log file
         */
        public LogManager(FileManager fileManager, string logfile)
        {
            m_FileManager = fileManager;
            m_Logfile = logfile;
            fileManager.OpenLogFile(m_Logfile);
            m_LogPage = new Page(fileManager.BlockSize);
            int logsize = fileManager.GetBlocksCount(logfile);
            if (logsize == 0)
                m_CurrentBlockId = AppendNewBlock();
            else
            {
                m_CurrentBlockId = BlockId.New(logfile, logsize - 1);
                fileManager.ReadBlock(m_CurrentBlockId, m_LogPage);
            }
        }

        public BlockId CurrentBlockId => m_CurrentBlockId;

        /**
         * Ensures that the log record corresponding to the
         * specified LSN has been written to disk.
         * All earlier log records will also be written to disk.
         * @param lsn the LSN of a log record
         */
        public void Flush(int lsn)
        {
            if (lsn >= m_LastSavedLSN)
                Flush();
        }

        internal LogIterator GetIterator()
        {
            Flush();
            return new LogIterator(m_FileManager, m_CurrentBlockId);
        }

        internal LogReverseIterator GetReverseIterator()
        {
            Flush();
            return new LogReverseIterator(m_FileManager, BlockId.New(m_Logfile, 0));
        }

        /**
         * Appends a log record to the log buffer. 
         * The record consists of an arbitrary array of bytes. 
         * Log records are written right to left in the buffer.
         * The size of the record is written before the bytes.
         * The beginning of the buffer contains the location
         * of the last-written record (the "boundary").
         * Storing the records backwards makes it easy to read
         * them in reverse order.
         * @param logrec a byte buffer containing the bytes.
         * @return the LSN of the final value
         */
        public int Append(byte[] logRecord)
        {
            lock(logManagerLock)
            {
                int boundary = m_LogPage.GetInt(0);
                int recsize = logRecord.Length;
                int bytesneeded = recsize + sizeof(int);
                if (boundary - bytesneeded < sizeof(int))
                { // the log record doesn't fit,
                    Flush();        // so move to the next block.
                    m_CurrentBlockId = AppendNewBlock();
                    boundary = m_LogPage.GetInt(0);
                }
                int recpos = boundary - bytesneeded;

                m_LogPage.SetBytes(recpos, logRecord);
                m_LogPage.SetInt(0, recpos); // the new boundary
                m_LatestLSN++;
                return m_LatestLSN;
            }
        }

        /**
         * Initialize the bytebuffer and append it to the log file.
         */
        private BlockId AppendNewBlock()
        {
            BlockId blk = m_FileManager.AppendNewBlock(m_Logfile);
            m_LogPage.SetInt(0, m_FileManager.BlockSize);
            m_FileManager.WritePage(m_LogPage, blk);
            return blk;
        }

        /**
         * Write the buffer to the log file.
         */
        internal void Flush()
        {
            m_FileManager.WritePage(m_LogPage, m_CurrentBlockId);
            m_LastSavedLSN = m_LatestLSN;
        }

        internal void Print()
        {
            Console.WriteLine();
            Console.WriteLine("Log records:");
            var iter = GetIterator();
            while (iter.HasNext())
            {
                byte[] bytes = iter.Next();
                LogRecord rec = LogRecord.createLogRecord(bytes);
                Console.WriteLine(rec.ToString());
            }
        }
    }
}
