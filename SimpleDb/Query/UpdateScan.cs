using SimpleDb.Record;
using SimpleDb.Types;
using System;

namespace SimpleDb.Query
{
    public interface UpdateScan : Scan
    {
        /**
         * Modify the field value of the current record.
         * @param fldname the name of the field
         * @param val the new value, expressed as a Constant
         */
        public void SetValue(String fldname, Constant val);

        /**
         * Modify the field value of the current record.
         * @param fldname the name of the field
         * @param val the new integer value
         */
        public void SetValue<T>(String fldname, T val);

        /**
         * Insert a new record somewhere in the scan.
         */
        public void Insert();

        /**
         * Delete the current record from the scan.
         */
        public void Delete();

        /**
         * Return the id of the current record.
         * @return the id of the current record
         */
        public RID GetRid();

        /**
         * Position the scan so that the current record has
         * the specified id.
         * @param rid the id of the desired record
         */
        public void MoveToRid(RID rid);
    }
}
