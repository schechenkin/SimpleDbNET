using SimpleDb.Query;
using SimpleDb.File;
using SimpleDb.Types;

namespace SimpleDb.Query
{
    public interface Scan
    {
        /**
         * Position the scan before its first record. A
         * subsequent call to next() will return the first record.
         */
        public void BeforeFirst();

        /**
         * Move the scan to the next record.
         * @return false if there is no next record
         */
        public bool Next();

        /**
         * Return the value of the specified integer field 
         * in the current record.
         * @param fldname the name of the field
         * @return the field's integer value in the current record
         */
        public int GetInt(string fldname);

        /**
         * Return the value of the specified string field 
         * in the current record.
         * @param fldname the name of the field
         * @return the field's string value in the current record
         */
        public string GetString(string fldname);

        public DateTime GetDateTime(string fldname);

        public bool IsNull(string fldname);

        /**
         * Return the value of the specified field in the current record.
         * The value is expressed as a Constant.
         * @param fldname the name of the field
         * @return the value of that field, expressed as a Constant.
         */
        public Constant GetValue(string fldname);

        /**
         * Return true if the scan has the specified field.
         * @param fldname the name of the field
         * @return true if the scan has that field
         */
        public bool HasField(string fldname);

        /**
         * Close the scan and its subscans, if any. 
         */
        public void Close();
    }
}
