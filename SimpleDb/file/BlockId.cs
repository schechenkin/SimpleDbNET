using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.file
{
    public class BlockId
    {
        private String filename;
        private int blknum;

        public BlockId(String filename, int blknum)
        {
            this.filename = filename;
            this.blknum = blknum;
        }

        public String fileName()
        {
            return filename;
        }

        public int number()
        {
            return blknum;
        }

        public override bool Equals(Object obj)
        {
            BlockId blk = (BlockId)obj;
            return filename.Equals(blk.filename) && blknum == blk.blknum;
        }

        public override String ToString()
        {
            return "[file " + filename + ", block " + blknum + "]";
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }
    }

}
