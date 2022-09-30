using SimpleDB.file;
using SimpleDB.Record;
using SimpleDB.Tx;
using System;
using System.Collections.Generic;

namespace SimpleDB.Metadata
{
    public class TableMgr
    {
        // The max characters a tablename or fieldname can have.
        public static int MAX_NAME = 16;
        private Layout tcatLayout, fcatLayout;

        /**
         * Create a new catalog manager for the database system.
         * If the database is new, the two catalog tables
         * are created.
         * @param isNew has the value true if the database is new
         * @param tx the startup transaction
         */
        public TableMgr(bool isNew, Transaction tx)
        {
            Schema tcatSchema = new Schema();
            tcatSchema.AddStringColumn("tblname", MAX_NAME);
            tcatSchema.AddIntColumn("slotsize");
            tcatLayout = new Layout(tcatSchema);

            Schema fcatSchema = new Schema();
            fcatSchema.AddStringColumn("tblname", MAX_NAME);
            fcatSchema.AddStringColumn("fldname", MAX_NAME);
            fcatSchema.AddIntColumn("type");
            fcatSchema.AddIntColumn("length");
            fcatSchema.AddIntColumn("offset");
            fcatSchema.AddIntColumn("nullable");
            fcatLayout = new Layout(fcatSchema);

            if (isNew)
            {
                createTable("tblcat", tcatSchema, tx);
                createTable("fldcat", fcatSchema, tx);
            }
        }

        /**
         * Create a new table having the specified name and schema.
         * @param tblname the name of the new table
         * @param sch the table's schema
         * @param tx the transaction creating the table
         */
        public void createTable(String tblname, Schema sch, Transaction tx)
        {
            Layout layout = new Layout(sch);
            // insert one record into tblcat
            TableScan tcat = new TableScan(tx, "tblcat", tcatLayout);
            tcat.insert();
            tcat.setString("tblname", tblname);
            tcat.setInt("slotsize", layout.slotSize());
            tcat.close();

            // insert a record into fldcat for each field
            TableScan fcat = new TableScan(tx, "fldcat", fcatLayout);
            foreach (String fldname in sch.ColumnNames())
            {
                fcat.insert();
                fcat.setString("tblname", tblname);
                fcat.setString("fldname", fldname);
                fcat.setInt("type", (int)sch.GetSqlType(fldname));
                fcat.setInt("length", sch.GetColumnLength(fldname));
                fcat.setInt("offset", layout.offset(fldname));
                fcat.setInt("nullable", sch.IsNullable(fldname) ? 1 : 0);
            }
            fcat.close();
        }

        /**
         * Retrieve the layout of the specified table
         * from the catalog.
         * @param tblname the name of the table
         * @param tx the transaction
         * @return the table's stored metadata
         */
        public Layout getLayout(string tblname, Transaction tx)
        {
            int size = -1;
            StringConstant tblnameConstant = new StringConstant(tblname);
            TableScan tcat = new TableScan(tx, "tblcat", tcatLayout);
            while (tcat.next())
                if (tcat.CompareString("tblname", tblnameConstant))
                {
                    size = tcat.getInt("slotsize");
                    break;
                }
            tcat.close();

            Schema sch = new Schema();
            Dictionary<string, int> offsets = new();
            TableScan fcat = new TableScan(tx, "fldcat", fcatLayout);

            while (fcat.next())
                if (fcat.CompareString("tblname", tblnameConstant))
                {
                    String fldname = fcat.getString("fldname");
                    int fldtype = fcat.getInt("type");
                    int fldlen = fcat.getInt("length");
                    int offset = fcat.getInt("offset");
                    offsets[fldname] = offset;
                    sch.AddColumn(fldname, (SqlType)fldtype, fldlen);
                }
            fcat.close();
            return new Layout(sch, offsets, size);
        }
    }
}
