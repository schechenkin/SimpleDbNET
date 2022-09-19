using SimpleDb.file;

namespace SimpleDbNET.Api.Tracking
{
    public class BlocksReadWriteTracker : IBlocksReadWriteTracker
    {
        static AsyncLocal<int> _blocksReadCounter = new AsyncLocal<int>();
        static AsyncLocal<int> _blocksWriteCounter = new AsyncLocal<int>();

        public int BlocksRead => _blocksReadCounter.Value;

        public int BlocksWrite => _blocksWriteCounter.Value;

        public void TrackBlockRead()
        {
            _blocksReadCounter.Value++;
        }

        public void TrackBlockWrite()
        {
            _blocksWriteCounter.Value++;
        }
    }
}
