using SimpleDb.Query;
using SimpleDb.File;
using SimpleDb.Types;

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

        public void BeforeFirst()
        {
            s.BeforeFirst();
        }

        public bool Next()
        {
            return s.Next();
        }

        public int GetInt(String fldname)
        {
            if (HasField(fldname))
                return s.GetInt(fldname);
            else
                throw new Exception("field " + fldname + " not found.");
        }

        public String GetString(String fldname)
        {
            if (HasField(fldname))
                return s.GetString(fldname);
            else
                throw new Exception("field " + fldname + " not found.");
        }

        public Constant GetValue(String fldname)
        {
            if (HasField(fldname))
                return s.GetValue(fldname);
            else
                throw new Exception("field " + fldname + " not found.");
        }

        public bool HasField(String fldname)
        {
            return fieldlist.Contains(fldname);
        }

        public void Close()
        {
            s.Close();
        }

        /*public bool CompareString(string fldname, StringConstant val)
        {
            if (hasField(fldname))
                return s.CompareString(fldname, val);
            else
                throw new Exception("field " + fldname + " not found.");
        }*/

        public DateTime GetDateTime(string fldname)
        {
            if (HasField(fldname))
                return s.GetDateTime(fldname);
            else
                throw new Exception("field " + fldname + " not found.");
        }

        public bool IsNull(string fldname)
        {
            if (HasField(fldname))
                return s.IsNull(fldname);
            else
                throw new Exception("field " + fldname + " not found.");
        }
    }
}
