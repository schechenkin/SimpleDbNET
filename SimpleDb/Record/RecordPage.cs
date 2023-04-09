using SimpleDb.File;
using SimpleDb.Transactions;
using SimpleDb.Types;
using System;

namespace SimpleDb.Record
{
    public struct RecordPage
    {
        public static int EMPTY = 0, USED = 1;
        private Transaction tx;
        private BlockId blk;
        private Layout layout;

        public RecordPage(Transaction tx, in BlockId blk, Layout layout)
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
        public DbString getString(int slot, String fldname)
        {
            int fldpos = offset(slot) + layout.offset(fldname);
            return tx.GetString(blk, fldpos);
        }

        public DateTime getDateTime(int slot, String fldname)
        {
            int fldpos = offset(slot) + layout.offset(fldname);
            return tx.GetDateTime(blk, fldpos);
        }

        public void SetValue<T>(int slot, String fldname, T value)
        {
            int fldpos = offset(slot) + layout.offset(fldname);
            tx.SetValue(blk, fldpos, value, true);
            setNotNull(slot, fldname);
        }

        public void setNull(int slot, String fldname)
        {
            var index = layout.bitLocation(fldname);
            tx.SetBit(blk, offset(slot) + Layout.NullBytesFlagsOffset, index, true, true);
        }

        private void setNotNull(int slot, String fldname)
        {
            var index = layout.bitLocation(fldname);
            tx.SetBit(blk, offset(slot) + Layout.NullBytesFlagsOffset, index, false, true);
        }

        public bool isNull(int slot, String fldname)
        {
            var index = layout.bitLocation(fldname);
            return tx.GetBitValue(blk, offset(slot) + Layout.NullBytesFlagsOffset, index);
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
                tx.SetValue(blk, offset(slot), EMPTY, false);
                Schema sch = layout.schema();
                foreach (string fldname in sch.ColumnNames())
                {
                    int fldpos = offset(slot) + layout.offset(fldname);
                    if (sch.GetSqlType(fldname) == SqlType.INTEGER)
                        tx.SetValue(blk, fldpos, 0, false);
                    else
                        tx.SetValue(blk, fldpos, "", false);
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
            tx.SetValue(blk, offset(slot), flag, true);
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
