using System.Diagnostics;

namespace SimpleDB.file
{
    public record BlockId
    {
        public string FileName { get; }
        public int Number { get; private set; }

        public static BlockId New(string fileName, int blockNumber)
        {
            Debug.Assert(blockNumber >= 0);

            return new BlockId(fileName, blockNumber);
        }

        public static BlockId Dummy(string fileName)
        {
            return new BlockId(fileName, -1);
        }

        private BlockId(string fileName, int blockNumber)
        {
            Debug.Assert(!string.IsNullOrEmpty(fileName));

            FileName = fileName;
            Number = blockNumber;
        }

        public override string ToString()
        {
            return "[file " + FileName + ", block " + Number + "]";
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }

        public void SetNumber(int number)
        {
            Number = number;
        }
    }

}
