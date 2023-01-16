using SimpleDb.Query;
using SimpleDB.file;

namespace SimpleDB.Query
{
    public class ProjectScan : Scan
    {
        private Scan s;
        private List<String> fieldlist;

        /**
         * Create a project scan having the specified
         * underlying scan and field list.
         * @param s the underlying scan
         * @param fieldlist the list of field names
         */
        public ProjectScan(Scan s, List<String> fieldlist)
        {
            this.s = s;
            this.fieldlist = fieldlist;
        }

        public void beforeFirst()
        {
            s.beforeFirst();
        }

        public bool next()
        {
            return s.next();
        }

        public int getInt(String fldname)
        {
            if (hasField(fldname))
                return s.getInt(fldname);
            else
                throw new Exception("field " + fldname + " not found.");
        }

        public String getString(String fldname)
        {
            if (hasField(fldname))
                return s.getString(fldname);
            else
                throw new Exception("field " + fldname + " not found.");
        }

        public Constant getVal(String fldname)
        {
            if (hasField(fldname))
                return s.getVal(fldname);
            else
                throw new Exception("field " + fldname + " not found.");
        }

        public ConstantRefStruct getVal2(string fldname)
        {
            if (hasField(fldname))
                return s.getVal2(fldname);
            else
                throw new Exception("field " + fldname + " not found.");
        }

        public bool hasField(String fldname)
        {
            return fieldlist.Contains(fldname);
        }

        public void close()
        {
            s.close();
        }

        public bool CompareString(string fldname, StringConstant val)
        {
            if (hasField(fldname))
                return s.CompareString(fldname, val);
            else
                throw new Exception("field " + fldname + " not found.");
        }

        public DateTime getDateTime(string fldname)
        {
            if (hasField(fldname))
                return s.getDateTime(fldname);
            else
                throw new Exception("field " + fldname + " not found.");
        }

        public bool isNull(string fldname)
        {
            if (hasField(fldname))
                return s.isNull(fldname);
            else
                throw new Exception("field " + fldname + " not found.");
        }
    }
}
