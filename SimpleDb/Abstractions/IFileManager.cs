using SimpleDb.File;

namespace SimpleDb.Abstractions;

public interface IFileManager
{
    void ReadPage(in BlockId blockId, in Page page);
    void WritePage(in BlockId blockId, in Page page, bool forceWriteOnDisk = false);
    int GetBlocksCount(string fileName);
    BlockId AppendNewBlock(string m_Logfile);
    void OpenFile(string fileName);
    void OpenTablesFiles();
    void Shrink(string fileName);
    int BlockSize {get;}
    void FlushTableFilesToDisk();
}
