using SimpleDb.Log;

namespace SimpleDb.Abstractions;

public interface ILogManager
{
    void Flush(LSN lsn, bool boolForceWriteOnDisk);
    void Flush(bool forceWriteOnDisk);
    LSN Append(byte[] data);

    LogIterator GetIterator();

    LogReverseIterator GetReverseIterator();

    void Shrink();
}
