﻿using SimpleDb.Types;
using SimpleDB.Query;

namespace SimpleDb.Indexes.Btree
{
    public class DirEntry
    {
        private Constant dataval;
        private int blocknum;

        /**
         * Creates a new entry for the specified dataval and block number.
         * @param dataval the dataval
         * @param blocknum the block number
         */
        public DirEntry(Constant dataval, int blocknum)
        {
            this.dataval = dataval;
            this.blocknum = blocknum;
        }

        /**
         * Returns the dataval component of the entry
         * @return the dataval component of the entry
         */
        public Constant dataVal()
        {
            return dataval;
        }

        /**
         * Returns the block number component of the entry
         * @return the block number component of the entry
         */
        public int blockNumber()
        {
            return blocknum;
        }
    }
}
