using SimpleDb.Query;
using SimpleDb.Record;
using SimpleDb.Types;

namespace SimpleDB.Query
{
    public class SelectScan : Scan
    {
		private Scan innerScan;
		private Predicate pred;

		/**
		 * Create a select scan having the specified underlying
		 * scan and predicate.
		 * @param s the scan of the underlying query
		 * @param pred the selection predicate
		 */
		public SelectScan(Scan scan, Predicate pred)
		{
			this.innerScan = scan;
			this.pred = pred;
		}

		// Scan methods

		public void BeforeFirst()
		{
			innerScan.BeforeFirst();
		}

		public bool Next()
		{
			while (innerScan.Next())
			{
				if (pred.isSatisfied(innerScan))
					return true;
			}
			return false;
		}

		public int GetInt(String fldname)
		{
			return innerScan.GetInt(fldname);
		}

		public String GetString(String fldname)
		{
			return innerScan.GetString(fldname);
		}

		public Constant GetValue(String fldname)
		{
			return innerScan.GetValue(fldname);
		}

        /*public ConstantRefStruct getVal2(string fldname)
        {
            return s.getVal2(fldname);
        }*/

        public bool HasField(String fldname)
		{
			return innerScan.HasField(fldname);
		}

		public void Close()
		{
			innerScan.Close();
		}

		// UpdateScan method

		public void setVal(String fldname, Constant val)
		{
			UpdateScan us = (UpdateScan)innerScan;
			us.SetValue(fldname, val);
		}

		public void delete()
		{
			UpdateScan us = (UpdateScan)innerScan;
			us.Delete();
		}

		public void insert()
		{
			UpdateScan us = (UpdateScan)innerScan;
			us.Insert();
		}

		public RID getRid()
		{
			UpdateScan us = (UpdateScan)innerScan;
			return us.GetRid();
		}

		public void moveToRid(RID rid)
		{
			UpdateScan us = (UpdateScan)innerScan;
			us.MoveToRid(rid);
		}

        public DateTime GetDateTime(string fldname)
        {
			return innerScan.GetDateTime(fldname);
		}

        public bool IsNull(string fldname)
        {
            return innerScan.IsNull(fldname);
        }
    }
}
