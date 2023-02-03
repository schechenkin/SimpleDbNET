using SimpleDB.file;
using SimpleDB.Query;
using SimpleDB.Record;
using SimpleDB.Tx;
using System.Diagnostics;
using SimpleDB;

namespace SimpleDb.Indexes.Btree
{
    /**
     * B-tree directory and leaf pages have many commonalities:
     * in particular, their records are stored in sorted order, 
     * and pages split when full.
     * A BTNode object contains this common functionality.
     * @author Edward Sciore
     */
    public class BTPage
    {
        private Transaction tx;
        private BlockId? currentblk;
        private Layout layout;

        /**
         * Open a node for the specified B-tree block.
         * @param currentblk a reference to the B-tree block
         * @param layout the metadata for the particular B-tree file
         * @param tx the calling transaction
         */
        public BTPage(Transaction tx, in BlockId currentblk, Layout layout)
        {
            this.tx = tx;
            this.currentblk = currentblk;
            this.layout = layout;
            tx.PinBlock(currentblk);
        }

        /**
         * Calculate the position where the first record having
         * the specified search key should be, then returns
         * the position before it.
         * @param searchkey the search key
         * @return the position before where the search key goes
         */
        public int findSlotBefore(Constant searchkey)
        {
            int slot = 0;
            while (slot < getNumRecs() && getDataVal(slot).CompareTo(searchkey) < 0)
                slot++;
            return slot - 1;
        }

        /**
         * Close the page by unpinning its buffer.
         */
        public void close()
        {
            if (currentblk != null)
                tx.UnpinBlock(currentblk.Value);
            currentblk = null;
        }

        /**
         * Return true if the block is full.
         * @return true if the block is full
         */
        public bool isFull()
        {
            return slotpos(getNumRecs() + 1) >= tx.blockSize();
        }

        /**
         * Split the page at the specified position.
         * A new page is created, and the records of the page
         * starting at the split position are transferred to the new page.
         * @param splitpos the split position
         * @param flag the initial value of the flag field
         * @return the reference to the new block
         */
        public BlockId split(int splitpos, int flag)
        {
            BlockId newblk = appendNew(flag);
            BTPage newpage = new BTPage(tx, newblk, layout);
            transferRecs(splitpos, newpage);
            newpage.setFlag(flag);
            newpage.close();
            return newblk;
        }

        /**
         * Return the dataval of the record at the specified slot.
         * @param slot the integer slot of an index record
         * @return the dataval of the record at that slot
         */
        public Constant getDataVal(int slot)
        {
            return getVal(slot, "dataval");
        }

        /**
         * Return the value of the page's flag field
         * @return the value of the page's flag field
         */
        public int getFlag()
        {
            Debug.Assert(this.currentblk != null);
            return tx.GetInt(currentblk.Value, 0);
        }

        /**
         * Set the page's flag field to the specified value
         * @param val the new value of the page flag
         */
        public void setFlag(int val)
        {
            Debug.Assert(this.currentblk != null);
            tx.SetInt(currentblk.Value, 0, val, true);
        }

        /**
         * Append a new block to the end of the specified B-tree file,
         * having the specified flag value.
         * @param flag the initial value of the flag
         * @return a reference to the newly-created block
         */
        public BlockId appendNew(int flag)
        {
            Debug.Assert(this.currentblk != null);
            BlockId blk = tx.append(currentblk.Value.FileName);
            tx.PinBlock(blk);
            format(blk, flag);
            return blk;
        }

        public void format(in BlockId blk, int flag)
        {
            tx.SetInt(blk, 0, flag, false);
            tx.SetInt(blk, sizeof(int), 0, false);  // #records = 0
            int recsize = layout.slotSize();
            for (int pos = 2 * sizeof(int); pos + recsize <= tx.blockSize(); pos += recsize)
                makeDefaultRecord(blk, pos);
        }

        private void makeDefaultRecord(BlockId blk, int pos)
        {
            foreach (String fldname in layout.schema().ColumnNames())
            {
                int offset = layout.offset(fldname);

                switch (layout.schema().GetSqlType(fldname))
                {
                    case SqlType.INTEGER:
                        tx.SetInt(blk, pos + offset, 0, false);
                        break;
                    case SqlType.VARCHAR:
                        tx.SetString(blk, pos + offset, "", false);
                        break;
                    case SqlType.DATETIME:
                        tx.SetDateTime(blk, pos + offset, new DateTime(), false);
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
        }
        // Methods called only by BTreeDir

        /**
         * Return the block number stored in the index record 
         * at the specified slot.
         * @param slot the slot of an index record
         * @return the block number stored in that record
         */
        public int getChildNum(int slot)
        {
            return getInt(slot, "block");
        }

        /**
         * Insert a directory entry at the specified slot.
         * @param slot the slot of an index record
         * @param val the dataval to be stored
         * @param blknum the block number to be stored
         */
        public void insertDir(int slot, Constant val, int blknum)
        {
            insert(slot);
            setVal(slot, "dataval", val);
            setInt(slot, "block", blknum);
        }

        // Methods called only by BTreeLeaf

        /**
         * Return the dataRID value stored in the specified leaf index record.
         * @param slot the slot of the desired index record
         * @return the dataRID value store at that slot
         */
        public RID getDataRid(int slot)
        {
            return new RID(getInt(slot, "block"), getInt(slot, "id"));
        }

        /**
         * Insert a leaf index record at the specified slot.
         * @param slot the slot of the desired index record
         * @param val the new dataval
         * @param rid the new dataRID
         */
        public void insertLeaf(int slot, Constant val, RID rid)
        {
            insert(slot);
            setVal(slot, "dataval", val);
            setInt(slot, "block", rid.blockNumber());
            setInt(slot, "id", rid.slot());
        }

        /**
         * Delete the index record at the specified slot.
         * @param slot the slot of the deleted index record
         */
        public void delete(int slot)
        {
            for (int i = slot + 1; i < getNumRecs(); i++)
                copyRecord(i, i - 1);
            setNumRecs(getNumRecs() - 1);
            return;
        }

        /**
         * Return the number of index records in this page.
         * @return the number of index records in this page
         */
        public int getNumRecs()
        {
            Debug.Assert(this.currentblk != null);
            return tx.GetInt(currentblk.Value, sizeof(int));
        }

        // Private methods

        private int getInt(int slot, String fldname)
        {
            int pos = fldpos(slot, fldname);
            Debug.Assert(this.currentblk != null);
            return tx.GetInt(currentblk.Value, pos);
        }

        private String getString(int slot, String fldname)
        {
            int pos = fldpos(slot, fldname);
            Debug.Assert(this.currentblk != null);
            return tx.GetString(currentblk.Value, pos);
        }

        private DateTime getDateTime(int slot, String fldname)
        {
            int pos = fldpos(slot, fldname);
            Debug.Assert(this.currentblk != null);
            return tx.GetDateTime(currentblk.Value, pos);
        }

        private Constant getVal(int slot, String fldname)
        {
            var type = layout.schema().GetSqlType(fldname);

            switch (layout.schema().GetSqlType(fldname))
            {
                case SqlType.INTEGER:
                    return new Constant(getInt(slot, fldname));
                case SqlType.VARCHAR:
                    return new Constant(getString(slot, fldname));
                case SqlType.DATETIME:
                    return new Constant(getDateTime(slot, fldname));
                default:
                    throw new NotImplementedException();
            }
        }

        private void setInt(int slot, String fldname, int val)
        {
            int pos = fldpos(slot, fldname);
            Debug.Assert(this.currentblk != null);
            tx.SetInt(currentblk.Value, pos, val, true);
        }

        private void setString(int slot, String fldname, String val)
        {
            int pos = fldpos(slot, fldname);
            Debug.Assert(this.currentblk != null);
            tx.SetString(currentblk.Value, pos, val, true);
        }

        private void setDateTime(int slot, String fldname, DateTime val)
        {
            int pos = fldpos(slot, fldname);
            Debug.Assert(this.currentblk != null);
            tx.SetDateTime(currentblk.Value, pos, val, true);
        }

        private void setVal(int slot, String fldname, Constant val)
        {
            switch (layout.schema().GetSqlType(fldname))
            {
                case SqlType.INTEGER:
                    setInt(slot, fldname, val.asInt());
                    break;
                case SqlType.VARCHAR:
                    setString(slot, fldname, val.asString());
                    break;
                case SqlType.DATETIME:
                    setDateTime(slot, fldname, val.asDateTime());
                    break;
                default:
                    throw new NotImplementedException();
            }
        }

        private void setNumRecs(int n)
        {
            Debug.Assert(this.currentblk != null);
            tx.SetInt(currentblk.Value, sizeof(int), n, true);
        }

        private void insert(int slot)
        {
            for (int i = getNumRecs(); i > slot; i--)
                copyRecord(i - 1, i);
            setNumRecs(getNumRecs() + 1);
        }

        private void copyRecord(int from, int to)
        {
            Schema sch = layout.schema();
            foreach (string fldname in sch.ColumnNames())
                setVal(to, fldname, getVal(from, fldname));
        }

        private void transferRecs(int slot, BTPage dest)
        {
            int destslot = 0;
            while (slot < getNumRecs())
            {
                dest.insert(destslot);
                Schema sch = layout.schema();
                foreach (string fldname in sch.ColumnNames())
                    dest.setVal(destslot, fldname, getVal(slot, fldname));
                delete(slot);
                destslot++;
            }
        }

        private int fldpos(int slot, string fldname)
        {
            int offset = layout.offset(fldname);
            return slotpos(slot) + offset;
        }

        private int slotpos(int slot)
        {
            int slotsize = layout.slotSize();
            return sizeof(int) + sizeof(int) + (slot * slotsize);
        }
    }

}
