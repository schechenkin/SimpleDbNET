using SimpleDb.Query;
using SimpleDb.File;
using SimpleDb.Transactions;
using SimpleDb.Types;

namespace SimpleDb.Record
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
            if (tx.Size(filename) == 0)
                moveToNewBlock();
            else
                moveToBlock(0);
        }

        // Methods that implement Scan

        public void BeforeFirst()
        {
            moveToBlock(0);
        }

        public bool Next()
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

        public int GetInt(string fldname)
        {
            return recordPage.getInt(currentslot, fldname);
        }

        public DbString GetString(string fldname)
        {
            return recordPage.getString(currentslot, fldname);
        }

        public DateTime GetDateTime(string fldname)
        {
            return recordPage.getDateTime(currentslot, fldname);
        }

        public Constant GetValue(string fldname)
        {
            if (IsNull(fldname))
                return new NULL();

            var sqlType = layout.schema().GetSqlType(fldname);
            if (sqlType == SqlType.INTEGER)
                return GetInt(fldname);
            else if(sqlType == SqlType.VARCHAR)
                return (DbString)GetString(fldname);
            else if(sqlType == SqlType.DATETIME)
                return GetDateTime(fldname);
            else
                throw new NotImplementedException();
        }

        public void setNull(string fldname)
        {
            recordPage.SetNull(currentslot, fldname);
        }

        public bool IsNull(string fldname)
        {
            return recordPage.IsNull(currentslot, fldname);
        }

        public bool HasField(string fldname)
        {
            return layout.schema().HasField(fldname);
        }

        public void Close()
        {
            if(!recordPage.block().isNull())
                tx.UnpinBlock(recordPage.block());
        }

        public void SetValue<T>(string fieldName, T value)
        {
            recordPage.SetValue(currentslot, fieldName, value);
        }

        public void SetValue(string fldname, Constant val)
        {
            if(val.IsNull)
                setNull(fldname);
            else
            {
                var sqlType = layout.schema().GetSqlType(fldname);

                if (sqlType == SqlType.INTEGER)
                    SetValue(fldname, val.AsInt);
                else if (sqlType == SqlType.VARCHAR)
                    SetValue(fldname, val.AsString);
                else if (sqlType == SqlType.DATETIME)
                    SetValue(fldname, val.AsDateTime);
                else
                    throw new NotImplementedException();
            }
        }

        public void Insert()
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

        public void Delete()
        {
            recordPage.Delete(currentslot);
        }

        public void MoveToRid(RID rid)
        {
            Close();
            BlockId blk = BlockId.New(filename, rid.blockNumber());
            recordPage = new RecordPage(tx, blk, layout);
            currentslot = rid.slot();
        }

        public RID GetRid()
        {
            return new RID((int)recordPage.block().Number, currentslot);
        }

        // Private auxiliary methods

        private void moveToBlock(int blknum)
        {
            Close();
            BlockId block = BlockId.New(filename, blknum);
            recordPage = new RecordPage(tx, block, layout);
            currentslot = -1;
        }

        private void moveToNewBlock()
        {
            Close();
            BlockId blk = tx.Append(filename);
            recordPage = new RecordPage(tx, blk, layout);
            recordPage.format();
            currentslot = -1;
        }

        private bool atLastBlock()
        {
            return recordPage.block().Number == tx.Size(filename) - 1;
        }
    }
}
