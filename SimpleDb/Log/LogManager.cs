using SimpleDb.Abstractions;
using SimpleDb.File;

namespace SimpleDb.Log;

public class LogManager : ILogManager
{
    private readonly IFileManager fileManager_;
    private readonly string logFile_;
    private readonly Page m_LogPage;
    private BlockId currentBlockId_;
    private LSN latestLSN_ = 0;
    private LSN lastSavedLSN_ = 0;
    private object mutex_ = new object();

    /**
     * Creates the manager for the specified log file.
     * If the log file does not yet exist, it is created
     * with an empty first block.
     * @param FileMgr the file manager
     * @param logfile the name of the log file
     */
    public LogManager(IFileManager fileManager, string logfile)
    {
        fileManager_ = fileManager;
        logFile_ = logfile;
        fileManager.OpenFile(logFile_);
        m_LogPage = new Page(1024*1024*16);
        int logsize = fileManager.GetBlocksCount(logfile);
        if (logsize == 0)
            currentBlockId_ = AppendNewBlock();
        else
        {
            currentBlockId_ = BlockId.New(logfile, logsize - 1);
            fileManager.ReadPage(currentBlockId_, m_LogPage);
        }
    }

    public BlockId CurrentBlockId => currentBlockId_;

    /**
     * Ensures that the log record corresponding to the
     * specified LSN has been written to disk.
     * All earlier log records will also be written to disk.
     * @param lsn the LSN of a log record
     */
    public void Flush(LSN lsn)
    {
        if (lsn >= lastSavedLSN_)
            Flush();
    }

    public LogIterator GetIterator()
    {
        Flush();
        return new LogIterator(fileManager_, BlockId.New(logFile_, 0));
    }

    public LogReverseIterator GetReverseIterator()
    {
        Flush();
        return new LogReverseIterator(fileManager_, currentBlockId_);
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
    public LSN Append(byte[] logRecord)
    {
        lock (mutex_)
        {
            int boundary = m_LogPage.GetInt(0);
            int recsize = logRecord.Length;
            int bytesneeded = recsize + sizeof(int);
            if (boundary - bytesneeded < sizeof(int))
            { // the log record doesn't fit,
                Flush();        // so move to the next block.
                currentBlockId_ = AppendNewBlock();
                boundary = m_LogPage.GetInt(0);
            }
            int recpos = boundary - bytesneeded;

            m_LogPage.SetBytes(recpos, logRecord);
            m_LogPage.SetInt(0, recpos); // the new boundary
            latestLSN_++;
            return latestLSN_;
        }
    }

    /**
     * Initialize the bytebuffer and append it to the log file.
     */
    private BlockId AppendNewBlock()
    {
        BlockId blk = fileManager_.AppendNewBlock(logFile_);
        m_LogPage.SetInt(0, fileManager_.BlockSize);
        fileManager_.WritePage(blk, m_LogPage);
        return blk;
    }

    /**
     * Write the buffer to the log file.
     */
    internal void Flush()
    {
        fileManager_.WritePage(currentBlockId_, m_LogPage);
        lastSavedLSN_ = latestLSN_;
    }

    /*internal void Print()
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
    }*/
}
