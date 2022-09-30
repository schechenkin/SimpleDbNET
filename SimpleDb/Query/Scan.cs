using SimpleDB.file;

namespace SimpleDB.Query
{
    public interface Scan
    {
        /**
         * Position the scan before its first record. A
         * subsequent call to next() will return the first record.
         */
        public void beforeFirst();

        /**
         * Move the scan to the next record.
         * @return false if there is no next record
         */
        public bool next();

        /**
         * Return the value of the specified integer field 
         * in the current record.
         * @param fldname the name of the field
         * @return the field's integer value in the current record
         */
        public int getInt(string fldname);

        /**
         * Return the value of the specified string field 
         * in the current record.
         * @param fldname the name of the field
         * @return the field's string value in the current record
         */
        public string getString(string fldname);


        public bool CompareString(string fldname, StringConstant val);

        /**
         * Return the value of the specified field in the current record.
         * The value is expressed as a Constant.
         * @param fldname the name of the field
         * @return the value of that field, expressed as a Constant.
         */
        public Constant getVal(string fldname);

        /**
         * Return true if the scan has the specified field.
         * @param fldname the name of the field
         * @return true if the scan has that field
         */
        public bool hasField(string fldname);

        /**
         * Close the scan and its subscans, if any. 
         */
        public void close();
    }
}
