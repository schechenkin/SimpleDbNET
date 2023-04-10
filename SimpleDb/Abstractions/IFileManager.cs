using SimpleDb.File;

namespace SimpleDb.Abstractions;

public interface IFileManager
{
    void ReadPage(in BlockId blockId, in Page page);
    void WritePage(in BlockId blockId, in Page page);
    int GetBlocksCount(string fileName);
    BlockId AppendNewBlock(string m_Logfile);
    void OpenFile(string fileName);
    void Shrink(string fileName);
    int BlockSize {get;}
}
