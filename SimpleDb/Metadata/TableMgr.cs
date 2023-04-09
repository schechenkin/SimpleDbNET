using SimpleDb.File;
using SimpleDb.Record;
using SimpleDb.Transactions;
using SimpleDb.Types;
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
            tcat.Insert();
            tcat.SetValue("tblname", (DbString)tblname);
            tcat.SetValue("slotsize", layout.slotSize());
            tcat.Close();

            // insert a record into fldcat for each field
            TableScan fcat = new TableScan(tx, "fldcat", fcatLayout);
            foreach (String fldname in sch.ColumnNames())
            {
                fcat.Insert();
                fcat.SetValue("tblname", (DbString)tblname);
                fcat.SetValue("fldname", (DbString)fldname);
                fcat.SetValue("type", (int)sch.GetSqlType(fldname));
                fcat.SetValue("length", sch.GetColumnLength(fldname));
                fcat.SetValue("offset", layout.offset(fldname));
                fcat.SetValue("nullable", sch.IsNullable(fldname) ? 1 : 0);
            }
            fcat.Close();
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
            DbString tblnameConstant = new DbString(tblname);
            TableScan tcat = new TableScan(tx, "tblcat", tcatLayout);
            while (tcat.Next())
            {
                //if (tcat.CompareString("tblname", tblnameConstant))
                var x = tcat.GetString("tblname");
                var dbTableName = tcat.GetValue("tblname");
                if (tcat.GetValue("tblname") == tblnameConstant)
                {
                    size = tcat.GetInt("slotsize");
                    break;
                }
            }
            tcat.Close();

            Schema sch = new Schema();
            Dictionary<string, int> offsets = new();
            TableScan fcat = new TableScan(tx, "fldcat", fcatLayout);

            while (fcat.Next())
            {
                //if (fcat.CompareString("tblname", tblnameConstant))
                var dbtblname = fcat.GetValue("tblname") ;
                if (fcat.GetValue("tblname") == tblnameConstant)
                {
                    DbString fldname = fcat.GetString("fldname");
                    int fldtype = fcat.GetInt("type");
                    int fldlen = fcat.GetInt("length");
                    int offset = fcat.GetInt("offset");
                    offsets[fldname.GetString()] = offset;
                    sch.AddColumn(fldname.GetString(), (SqlType)fldtype, fldlen);
                }
            }
            fcat.Close();
            return new Layout(sch, offsets, size);
        }
    }
}
