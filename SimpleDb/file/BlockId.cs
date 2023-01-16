using System.Diagnostics;

namespace SimpleDB.file
{
    public readonly record struct BlockId
    {
        public readonly string FileName { get; }
        public readonly int Number { get;  }

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

        public bool isNull()
        {
            return string.IsNullOrEmpty(FileName);
        }

        public override string ToString()
        {
            return "[file " + FileName + ", block " + Number + "]";
        }
    }
}
