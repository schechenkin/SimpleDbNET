using SimpleDb.file;

namespace QueryPlannerTest
{
    internal class EmptyBlocksReadWriteTracker : IBlocksReadWriteTracker
    {
        public int BlocksRead { get; set; }

        public int BlocksWrite { get; set; }

        public void TrackBlockRead()
        {
            
        }

        public void TrackBlockWrite()
        {
            
        }
    }
}
