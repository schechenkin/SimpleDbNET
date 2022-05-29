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
        private RecordPage rp;
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
            currentslot = rp.nextAfter(currentslot);
            while (currentslot < 0)
            {
                if (atLastBlock())
                    return false;
                moveToBlock(rp.block().Number + 1);
                currentslot = rp.nextAfter(currentslot);
            }
            return true;
        }

        public int getInt(string fldname)
        {
            return rp.getInt(currentslot, fldname);
        }

        public string getString(string fldname)
        {
            return rp.getString(currentslot, fldname);
        }

        public Constant getVal(string fldname)
        {
            if (layout.schema().type(fldname) == SqlType.INTEGER)
                return new Constant(getInt(fldname));
            else
                return new Constant(getString(fldname));
        }

        public bool hasField(string fldname)
        {
            return layout.schema().hasField(fldname);
        }

        public void close()
        {
            if (rp != null)
                tx.unpin(rp.block());
        }

        // Methods that implement UpdateScan

        public void setInt(string fldname, int val)
        {
            rp.setInt(currentslot, fldname, val);
        }

        public void setString(string fldname, string val)
        {
            rp.setString(currentslot, fldname, val);
        }

        public void setVal(string fldname, Constant val)
        {
            if (layout.schema().type(fldname) == SqlType.INTEGER)
                setInt(fldname, val.asInt());
            else
                setString(fldname, val.asString());
        }

        public void insert()
        {
            currentslot = rp.insertAfter(currentslot);
            while (currentslot < 0)
            {
                if (atLastBlock())
                    moveToNewBlock();
                else
                    moveToBlock(rp.block().Number + 1);
                currentslot = rp.insertAfter(currentslot);
            }
        }

        public void delete()
        {
            rp.delete(currentslot);
        }

        public void moveToRid(RID rid)
        {
            close();
            BlockId blk = BlockId.New(filename, rid.blockNumber());
            rp = new RecordPage(tx, blk, layout);
            currentslot = rid.slot();
        }

        public RID getRid()
        {
            return new RID((int)rp.block().Number, currentslot);
        }

        // Private auxiliary methods

        private void moveToBlock(int blknum)
        {
            close();
            BlockId blk = BlockId.New(filename, blknum);
            rp = new RecordPage(tx, blk, layout);
            currentslot = -1;
        }

        private void moveToNewBlock()
        {
            close();
            BlockId blk = tx.append(filename);
            rp = new RecordPage(tx, blk, layout);
            rp.format();
            currentslot = -1;
        }

        private bool atLastBlock()
        {
            return rp.block().Number == tx.size(filename) - 1;
        }
    }
}
