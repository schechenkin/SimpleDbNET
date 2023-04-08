using SimpleDb.Query;
using SimpleDb.File;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SimpleDb.Types;

namespace SimpleDb.Query
{
    public class ProductScan : Scan
    {
        private Scan s1, s2;

        /**
         * Create a product scan having the two underlying scans.
         * @param s1 the LHS scan
         * @param s2 the RHS scan
         */
        public ProductScan(Scan s1, Scan s2)
        {
            this.s1 = s1;
            this.s2 = s2;
            BeforeFirst();
        }

        /**
         * Position the scan before its first record.
         * In particular, the LHS scan is positioned at 
         * its first record, and the RHS scan
         * is positioned before its first record.
         * @see simpledb.query.Scan#beforeFirst()
         */
        public void BeforeFirst()
        {
            s1.BeforeFirst();
            s1.Next();
            s2.BeforeFirst();
        }

        /**
         * Move the scan to the next record.
         * The method moves to the next RHS record, if possible.
         * Otherwise, it moves to the next LHS record and the
         * first RHS record.
         * If there are no more LHS records, the method returns false.
         * @see simpledb.query.Scan#next()
         */
        public bool Next()
        {
            if (s2.Next())
                return true;
            else
            {
                s2.BeforeFirst();
                return s2.Next() && s1.Next();
            }
        }

        /** 
         * Return the integer value of the specified field.
         * The value is obtained from whichever scan
         * contains the field.
         * @see simpledb.query.Scan#getInt(java.lang.String)
         */
        public int GetInt(String fldname)
        {
            if (s1.HasField(fldname))
                return s1.GetInt(fldname);
            else
                return s2.GetInt(fldname);
        }

        /** 
         * Returns the string value of the specified field.
         * The value is obtained from whichever scan
         * contains the field.
         * @see simpledb.query.Scan#getString(java.lang.String)
         */
        public String GetString(String fldname)
        {
            if (s1.HasField(fldname))
                return s1.GetString(fldname);
            else
                return s2.GetString(fldname);
        }

        /** 
         * Return the value of the specified field.
         * The value is obtained from whichever scan
         * contains the field.
         * @see simpledb.query.Scan#getVal(java.lang.String)
         */
        public Constant GetValue(String fldname)
        {
            if (s1.HasField(fldname))
                return s1.GetValue(fldname);
            else
                return s2.GetValue(fldname);
        }

        /*public ConstantRefStruct getVal2(string fldname)
        {
            if (s1.hasField(fldname))
                return s1.getVal2(fldname);
            else
                return s2.getVal2(fldname);
        }*/

        /**
         * Returns true if the specified field is in
         * either of the underlying scans.
         * @see simpledb.query.Scan#hasField(java.lang.String)
         */
        public bool HasField(String fldname)
        {
            return s1.HasField(fldname) || s2.HasField(fldname);
        }

        /**
         * Close both underlying scans.
         * @see simpledb.query.Scan#close()
         */
        public void Close()
        {
            s1.Close();
            s2.Close();
        }

        /*public bool CompareString(string fldname, StringConstant val)
        {
            if (s1.hasField(fldname))
                return s1.CompareString(fldname, val);
            else
                return s2.CompareString(fldname, val);
        }*/

        public DateTime GetDateTime(string fldname)
        {
            if (s1.HasField(fldname))
                return s1.GetDateTime(fldname);
            else
                return s2.GetDateTime(fldname);
        }

        public bool IsNull(string fldname)
        {
            if (s1.HasField(fldname))
                return s1.IsNull(fldname);
            else
                return s2.IsNull(fldname);
        }
    }
}
