namespace SimpleDb.file
{
    public interface IBlocksReadWriteTracker
    {
        void TrackBlockRead();
        void TrackBlockWrite();
        int BlocksRead { get; }
        int BlocksWrite { get; }
    }
}
