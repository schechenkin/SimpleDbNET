﻿using SimpleDB.Record;
using System;

namespace SimpleDB.Query
{
    public interface UpdateScan : Scan
    {
        /**
         * Modify the field value of the current record.
         * @param fldname the name of the field
         * @param val the new value, expressed as a Constant
         */
        public void setVal(String fldname, Constant val);

        /**
         * Modify the field value of the current record.
         * @param fldname the name of the field
         * @param val the new integer value
         */
        public void setInt(String fldname, int val);

        /**
         * Modify the field value of the current record.
         * @param fldname the name of the field
         * @param val the new string value
         */
        public void setString(String fldname, String val);

        /**
         * Insert a new record somewhere in the scan.
         */
        public void insert();

        /**
         * Delete the current record from the scan.
         */
        public void delete();

        /**
         * Return the id of the current record.
         * @return the id of the current record
         */
        public RID getRid();

        /**
         * Position the scan so that the current record has
         * the specified id.
         * @param rid the id of the desired record
         */
        public void moveToRid(RID rid);
    }
}
