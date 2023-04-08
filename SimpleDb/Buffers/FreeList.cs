using SimpleDb.Abstractions;

namespace SimpleDb.Buffers;

internal class FreeList
{
    private LinkedList<Buffer> m_FreeBuffers = new LinkedList<Buffer>();

    public int BufferCount => m_FreeBuffers.Count;

    public FreeList(int bufferCount, IFileManager fm, ILogManager lm)
    {
        for (int i = 0; i < bufferCount; i++)
            m_FreeBuffers.AddLast(new Buffer(fm, lm));
    }

    public bool TryGetBuffer(out Buffer? buffer)
    {
        if (m_FreeBuffers.First != null)
        {
            buffer = m_FreeBuffers.First.Value;
            m_FreeBuffers.RemoveFirst();
            return true;
        }
        else
        {
            buffer = null;
            return false;
        }
    }
}
