using SimpleDb.file;

namespace SimpleDbNET.UnitTests
{
    internal class TestBlocksReadWriteTracker : IBlocksReadWriteTracker
    {
        public int BlocksRead { get; set; }

        public int BlocksWrite { get; set; }

        public void TrackBlockRead()
        {
            BlocksRead++;
        }

        public void TrackBlockWrite()
        {
            BlocksWrite++;
        }
    }
}
