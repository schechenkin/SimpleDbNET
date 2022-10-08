using SimpleDB.Query;

namespace SimpleDB.QueryParser
{
    public class InsertData
    {
        private String tblname;
        private List<String> flds;
        private List<List<Constant>> _vals;

        /**
         * Saves the table name and the field and value lists.
         */
        public InsertData(String tblname, List<String> flds, List<List<Constant>> vals)
        {
            this.tblname = tblname;
            this.flds = flds;
            this._vals = vals;
        }

        /**
         * Returns the name of the affected table.
         * @return the name of the affected table
         */
        public String tableName()
        {
            return tblname;
        }

        /**
         * Returns a list of fields for which
         * values will be specified in the new record.
         * @return a list of field names
         */
        public List<String> fields()
        {
            return flds;
        }

        /**
         * Returns a list of values for the specified fields.
         * There is a one-one correspondence between this
         * list of values and the list of fields.
         * @return a list of Constant values.
         */
        public List<List<Constant>> vals()
        {
            return _vals;
        }
    }
}
