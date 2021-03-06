using SimpleDB.file;
using SimpleDB.Query;
using SimpleDB.Tx;
using System;

namespace SimpleDB.Record
{
    public class TableScan : UpdateScan
    {
        private Transaction tx;
        private Layout layout;
        private RecordPage recordPage;
        private string filename;
        private int currentslot;

        public TableScan(Transaction tx, string tblname, Layout layout)
        {
            this.tx = tx;
            this.layout = layout;
            filename = tblname + ".tbl";
            if (tx.size(filename) == 0)
                moveToNewBlock();
            else
                moveToBlock(0);
        }

        // Methods that implement Scan

        public void beforeFirst()
        {
            moveToBlock(0);
        }

        public bool next()
        {
            currentslot = recordPage.nextAfter(currentslot);
            while (currentslot < 0)
            {
                if (atLastBlock())
                    return false;
                moveToBlock(recordPage.block().Number + 1);
                currentslot = recordPage.nextAfter(currentslot);
            }
            return true;
        }

        public int getInt(string fldname)
        {
            return recordPage.getInt(currentslot, fldname);
        }

        public string getString(string fldname)
        {
            return recordPage.getString(currentslot, fldname);
        }

        public Constant getVal(string fldname)
        {
            if (layout.schema().GetSqlType(fldname) == SqlType.INTEGER)
                return new Constant(getInt(fldname));
            else
                return new Constant(getString(fldname));
        }

        public bool hasField(string fldname)
        {
            return layout.schema().HasField(fldname);
        }

        public void close()
        {
            if(recordPage.block() != null)
                tx.UnpinBlock(recordPage.block());
        }

        // Methods that implement UpdateScan

        public void setInt(string fldname, int val)
        {
            recordPage.setInt(currentslot, fldname, val);
        }

        public void setString(string fldname, string val)
        {
            recordPage.setString(currentslot, fldname, val);
        }

        public void setVal(string fldname, Constant val)
        {
            if (layout.schema().GetSqlType(fldname) == SqlType.INTEGER)
                setInt(fldname, val.asInt());
            else
                setString(fldname, val.asString());
        }

        public void insert()
        {
            currentslot = recordPage.insertAfter(currentslot);
            while (currentslot < 0)
            {
                if (atLastBlock())
                    moveToNewBlock();
                else
                    moveToBlock(recordPage.block().Number + 1);
                currentslot = recordPage.insertAfter(currentslot);
            }
        }

        public void delete()
        {
            recordPage.delete(currentslot);
        }

        public void moveToRid(RID rid)
        {
            close();
            BlockId blk = BlockId.New(filename, rid.blockNumber());
            recordPage = new RecordPage(tx, blk, layout);
            currentslot = rid.slot();
        }

        public RID getRid()
        {
            return new RID((int)recordPage.block().Number, currentslot);
        }

        // Private auxiliary methods

        private void moveToBlock(int blknum)
        {
            close();
            BlockId blk = BlockId.New(filename, blknum);
            recordPage = new RecordPage(tx, blk, layout);
            currentslot = -1;
        }

        private void moveToNewBlock()
        {
            close();
            BlockId blk = tx.append(filename);
            recordPage = new RecordPage(tx, blk, layout);
            recordPage.format();
            currentslot = -1;
        }

        private bool atLastBlock()
        {
            return recordPage.block().Number == tx.size(filename) - 1;
        }
    }
}
