namespace SimpleDB.Record
{
    public class Schema
    {
        private List<string> _fields = new ();
        private Dictionary<string, FieldInfo> info = new ();

        /**
         * Add a field to the schema having a specified
         * name, type, and length.
         * If the field type is "integer", then the length
         * value is irrelevant.
         * @param fldname the name of the field
         * @param type the type of the field, according to the constants in simpledb.sql.types
         * @param length the conceptual length of a string field.
         */
        public void AddColumn(string fldname, SqlType type, int length)
        {
            _fields.Add(fldname);
            info[fldname] =  new FieldInfo(type, length);
        }

        /**
         * Add an integer field to the schema.
         * @param fldname the name of the field
         */
        public void AddIntColumn(string fldname)
        {
            AddColumn(fldname, SqlType.INTEGER, sizeof(int));
        }

        /**
         * Add a string field to the schema.
         * The length is the conceptual length of the field.
         * For example, if the field is defined as varchar(8),
         * then its length is 8.
         * @param fldname the name of the field
         * @param length the number of chars in the varchar definition
         */
        public void AddStringColumn(string fldname, int length)
        {
            AddColumn(fldname, SqlType.VARCHAR, length);
        }

        /**
         * Add a field to the schema having the same
         * type and length as the corresponding field
         * in another schema.
         * @param fldname the name of the field
         * @param sch the other schema
         */
        public void AddColumn(string fldname, Schema sch)
        {
            SqlType type = sch.GetSqlType(fldname);
            int length = sch.GetColumnLength(fldname);
            AddColumn(fldname, type, length);
        }

        /**
         * Add all of the fields in the specified schema
         * to the current schema.
         * @param sch the other schema
         */
        public void AddAll(Schema sch)
        {
            foreach (string fldname in sch.ColumnNames())
                AddColumn(fldname, sch);
        }

        /**
         * Return a collection containing the name of
         * each field in the schema.
         * @return the collection of the schema's field names
         */
        public List<string> ColumnNames()
        {
            return _fields;
        }

        /**
         * Return true if the specified field
         * is in the schema
         * @param fldname the name of the field
         * @return true if the field is in the schema
         */
        public bool HasField(string fldname)
        {
            return _fields.Contains(fldname);
        }

        /**
         * Return the type of the specified field, using the
         * constants in {@link java.sql.Types}.
         * @param fldname the name of the field
         * @return the integer type of the field
         */
        public SqlType GetSqlType(string fldname)
        {
            return info[fldname].type;
        }

        /**
         * Return the conceptual length of the specified field.
         * If the field is not a string field, then
         * the return value is undefined.
         * @param fldname the name of the field
         * @return the conceptual length of the field
         */
        public int GetColumnLength(string fldname)
        {
            return info[fldname].length;
        }

        struct FieldInfo
        {
            internal SqlType type;
            internal int length;
            public FieldInfo(SqlType type, int length)
            {
                this.type = type;
                this.length = length;
            }
        }
    }
}
