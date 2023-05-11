using SimpleDb.Abstractions;
using SimpleDb.File;
using SimpleDb.Transactions;
using SimpleDb.Types;
using System;

namespace SimpleDb.Record
{
    public struct RecordPage
    {
        public static int EMPTY = 0, USED = 1;
        private Transaction transaction;
        private BlockId blk;
        private Layout layout;

        public static int TransactionNumberOffset = 0;

        public RecordPage(Transaction tx, in BlockId blk, Layout layout)
        {
            this.transaction = tx;
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
            return transaction.GetInt(blk, fldpos);
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
            return transaction.GetString(blk, fldpos);
        }

        public DateTime getDateTime(int slot, String fldname)
        {
            int fldpos = offset(slot) + layout.offset(fldname);
            return transaction.GetDateTime(blk, fldpos);
        }

        public void SetValue<T>(int slot, String fldname, T value)
        {
            int fldpos = offset(slot) + layout.offset(fldname);
            transaction.SetValue(blk, fldpos, value, true);
            SetNotNull(slot, fldname);
            SaveTransactionNumberIntoBlockHeader();
        }

        public void SetNull(int slot, String fldname)
        {
            var index = layout.bitLocation(fldname);
            transaction.SetBit(blk, offset(slot) + Layout.NullBytesFlagsOffset, index, true, true);
            SaveTransactionNumberIntoBlockHeader();
        }  
        private void SetNotNull(int slot, String fldname)
        {
            var index = layout.bitLocation(fldname);
            transaction.SetBit(blk, offset(slot) + Layout.NullBytesFlagsOffset, index, false, true);
            SaveTransactionNumberIntoBlockHeader();
        }

        private void SaveTransactionNumberIntoBlockHeader()
        {
            transaction.SetValue(blk, TransactionNumberOffset, transaction.Number, false);
        }     

        public bool IsNull(int slot, String fldname)
        {
            var index = layout.bitLocation(fldname);
            return transaction.GetBitValue(blk, offset(slot) + Layout.NullBytesFlagsOffset, index);
        }

        public void Delete(int slot)
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
                transaction.SetValue(blk, offset(slot), EMPTY, false);
                Schema sch = layout.schema();
                foreach (string fldname in sch.ColumnNames())
                {
                    int fldpos = offset(slot) + layout.offset(fldname);
                    if (sch.GetSqlType(fldname) == SqlType.INTEGER)
                        transaction.SetValue(blk, fldpos, 0, false);
                    else
                        transaction.SetValue(blk, fldpos, "", false);
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
            transaction.SetValue(blk, offset(slot), flag, true);
        }

        private int searchAfter(int slot, int flag)
        {
            slot++;
            while (isValidSlot(slot))
            {
                if (transaction.GetInt(blk, offset(slot)) == flag)
                    return slot;
                slot++;
            }
            return -1;
        }

        private bool isValidSlot(int slot)
        {
            return offset(slot + 1) <= transaction.BlockSize();
        }

        private int offset(int slot)
        {
            return TransactionNumber.Size() + slot * layout.slotSize();
        }
    }
}
