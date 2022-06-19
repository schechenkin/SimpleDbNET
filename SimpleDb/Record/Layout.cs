using SimpleDB.file;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.Record
{
    public class Layout
    {
        private Schema _schema;
        private Dictionary<string, int> _offsets;
        private int _slotsize;

        /**
         * This constructor creates a Layout object from a schema. 
         * This constructor is used when a table 
         * is created. It determines the physical offset of 
         * each field within the record.
         * @param tblname the name of the table
         * @param schema the schema of the table's records
         */
        public Layout(Schema schema)
        {
            _schema = schema;
            _offsets = new ();
            int pos = sizeof(int); // leave space for the empty/inuse flag
            foreach (string fldname in schema.ColumnNames())
            {
                _offsets[fldname] = pos;
                pos += lengthInBytes(fldname);
            }
            _slotsize = pos;
        }

        /**
         * Create a Layout object from the specified metadata.
         * This constructor is used when the metadata
         * is retrieved from the catalog.
         * @param tblname the name of the table
         * @param schema the schema of the table's records
         * @param offsets the already-calculated offsets of the fields within a record
         * @param recordlen the already-calculated length of each record
         */
        public Layout(Schema schema, Dictionary<string, int> offsets, int slotsize)
        {
            _schema = schema;
            _offsets = offsets;
            _slotsize = slotsize;
        }

        /**
         * Return the schema of the table's records
         * @return the table's record schema
         */
        public Schema schema()
        {
            return _schema;
        }

        /**
         * Return the offset of a specified field within a record
         * @param fldname the name of the field
         * @return the offset of that field within a record
         */
        public int offset(string fldname)
        {
            return _offsets[fldname];
        }

        /**
         * Return the size of a slot, in bytes.
         * @return the size of a slot
         */
        public int slotSize()
        {
            return _slotsize;
        }

        private int lengthInBytes(string fldname)
        {
            SqlType fldtype = _schema.GetSqlType(fldname);
            if (fldtype == SqlType.INTEGER)
                return sizeof(int);
            else if (fldtype == SqlType.VARCHAR)
                return Page.maxLength(_schema.GetColumnLength(fldname));
            else
                throw new NotImplementedException();
        }
    }
}
