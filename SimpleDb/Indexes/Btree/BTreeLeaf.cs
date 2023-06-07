﻿
using SimpleDb.File;
using SimpleDb.Record;
using SimpleDb.Transactions;
using SimpleDb.Types;

namespace SimpleDb.Indexes.Btree
{
    public class BTreeLeaf
    {
        private Transaction tx;
        private Layout layout;
        private Constant searchkey;
        private BTPage contents;
        private int currentslot;
        private string filename;

        /**
         * Opens a buffer to hold the specified leaf block.
         * The buffer is positioned immediately before the first record
         * having the specified search key (if any).
         * @param blk a reference to the disk block
         * @param layout the metadata of the B-tree leaf file
         * @param searchkey the search key value
         * @param tx the calling transaction
         */
        public BTreeLeaf(Transaction tx, in BlockId blk, Layout layout, Constant searchkey)
        {
            this.tx = tx;
            this.layout = layout;
            this.searchkey = searchkey;
            contents = new BTPage(tx, blk, layout);
            currentslot = contents.findSlotBefore(searchkey);
            filename = blk.FileName;
        }

        /**
         * Closes the leaf page.
         */
        public void close()
        {
            contents.close();
        }

        /**
         * Moves to the next leaf record having the 
         * previously-specified search key.
         * Returns false if there is no more such records.
         * @return false if there are no more leaf records for the search key
         */
        public bool next()
        {
            currentslot++;
            if (currentslot >= contents.getNumRecs())
                return tryOverflow();
            else if (contents.getDataVal(currentslot).Equals(searchkey))
                return true;
            else
                return tryOverflow();
        }

        /**
         * Returns the dataRID value of the current leaf record.
         * @return the dataRID of the current record
         */
        public RID getDataRid()
        {
            return contents.getDataRid(currentslot);
        }

        /**
         * Deletes the leaf record having the specified dataRID
         * @param datarid the dataRId whose record is to be deleted
         */
        public void delete(RID datarid)
        {
            while (next())
                if (getDataRid().Equals(datarid))
                {
                    contents.delete(currentslot);
                    return;
                }
        }

        /**
         * Inserts a new leaf record having the specified dataRID
         * and the previously-specified search key.
         * If the record does not fit in the page, then 
         * the page splits and the method returns the
         * directory entry for the new page;
         * otherwise, the method returns null.  
         * If all of the records in the page have the same dataval,
         * then the block does not split; instead, all but one of the
         * records are placed into an overflow block.
         * @param datarid the dataRID value of the new record
         * @return the directory entry of the newly-split page, if one exists.
         */
        public DirEntry insert(RID datarid)
        {
            if (contents.getFlag() >= 0 && contents.getDataVal(0).CompareTo(searchkey) > 0)
            {
                Constant firstval = contents.getDataVal(0);
                BlockId newblk = contents.split(0, contents.getFlag());
                currentslot = 0;
                contents.setFlag(-1);
                contents.insertLeaf(currentslot, searchkey, datarid);
                return new DirEntry(firstval, newblk.Number);
            }

            currentslot++;
            contents.insertLeaf(currentslot, searchkey, datarid);
            if (!contents.isFull())
                return null;
            // else page is full, so split it
            Constant firstkey = contents.getDataVal(0);
            Constant lastkey = contents.getDataVal(contents.getNumRecs() - 1);
            if (lastkey.Equals(firstkey))
            {
                // create an overflow block to hold all but the first record
                BlockId newblk = contents.split(1, contents.getFlag());
                contents.setFlag(newblk.Number);
                return null;
            }
            else
            {
                int splitpos = contents.getNumRecs() / 2;
                Constant splitkey = contents.getDataVal(splitpos);
                if (splitkey.Equals(firstkey))
                {
                    // move right, looking for the next key
                    while (contents.getDataVal(splitpos).Equals(splitkey))
                        splitpos++;
                    splitkey = contents.getDataVal(splitpos);
                }
                else
                {
                    // move left, looking for first entry having that key
                    while (contents.getDataVal(splitpos - 1).Equals(splitkey))
                        splitpos--;
                }
                BlockId newblk = contents.split(splitpos, -1);
                return new DirEntry(splitkey, newblk.Number);
            }
        }

        private bool tryOverflow()
        {
            Constant firstkey = contents.getDataVal(0);
            int flag = contents.getFlag();
            if (!searchkey.Equals(firstkey) || flag < 0)
                return false;
            contents.close();
            BlockId nextblk = BlockId.New(filename, flag);
            contents = new BTPage(tx, nextblk, layout);
            currentslot = 0;
            return true;
        }
    }
}
