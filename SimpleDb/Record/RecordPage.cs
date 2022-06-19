using SimpleDB.file;
using SimpleDB.Tx;
using System;

namespace SimpleDB.Record
{
    public struct RecordPage
    {
        public static int EMPTY = 0, USED = 1;
        private Transaction tx;
        private BlockId blk;
        private Layout layout;

        public RecordPage(Transaction tx, BlockId blk, Layout layout)
        {
            this.tx = tx;
            this.blk = blk;
            this.layout = layout;
            tx.PinBlock(blk);
        }

        /**
         * Return the integer value stored for the
         * specified field of a specified slot.
         * @param fldname the name of the field.
         * @return the integer stored in that field
         */
        public int getInt(int slot, String fldname)
        {
            int fldpos = offset(slot) + layout.offset(fldname);
            return tx.GetInt(blk, fldpos);
        }

        /**
         * Return the string value stored for the
         * specified field of the specified slot.
         * @param fldname the name of the field.
         * @return the string stored in that field
         */
        public String getString(int slot, String fldname)
        {
            int fldpos = offset(slot) + layout.offset(fldname);
            return tx.GetString(blk, fldpos);
        }

        /**
         * Store an integer at the specified field
         * of the specified slot.
         * @param fldname the name of the field
         * @param val the integer value stored in that field
         */
        public void setInt(int slot, String fldname, int val)
        {
            int fldpos = offset(slot) + layout.offset(fldname);
            tx.SetInt(blk, fldpos, val, true);
        }

        /**
         * Store a string at the specified field
         * of the specified slot.
         * @param fldname the name of the field
         * @param val the string value stored in that field
         */
        public void setString(int slot, String fldname, String val)
        {
            int fldpos = offset(slot) + layout.offset(fldname);
            tx.SetString(blk, fldpos, val, true);
        }

        public void delete(int slot)
        {
            setFlag(slot, EMPTY);
        }

        /** Use the layout to format a new block of records.
         *  These values should not be logged 
         *  (because the old values are meaningless).
         */
        public void format()
        {
            int slot = 0;
            while (isValidSlot(slot))
            {
                tx.SetInt(blk, offset(slot), EMPTY, false);
                Schema sch = layout.schema();
                foreach (string fldname in sch.ColumnNames())
                {
                    int fldpos = offset(slot) + layout.offset(fldname);
                    if (sch.GetSqlType(fldname) == SqlType.INTEGER)
                        tx.SetInt(blk, fldpos, 0, false);
                    else
                        tx.SetString(blk, fldpos, "", false);
                }
                slot++;
            }
        }

        public int nextAfter(int slot)
        {
            return searchAfter(slot, USED);
        }

        public int insertAfter(int slot)
        {
            int newslot = searchAfter(slot, EMPTY);
            if (newslot >= 0)
                setFlag(newslot, USED);
            return newslot;
        }

        public BlockId block()
        {
            return blk;
        }

        // Private auxiliary methods

        /**
         * Set the record's empty/inuse flag.
         */
        private void setFlag(int slot, int flag)
        {
            tx.SetInt(blk, offset(slot), flag, true);
        }

        private int searchAfter(int slot, int flag)
        {
            slot++;
            while (isValidSlot(slot))
            {
                if (tx.GetInt(blk, offset(slot)) == flag)
                    return slot;
                slot++;
            }
            return -1;
        }

        private bool isValidSlot(int slot)
        {
            return offset(slot + 1) <= tx.blockSize();
        }

        private int offset(int slot)
        {
            return slot * layout.slotSize();
        }
    }
}
