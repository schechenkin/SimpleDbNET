using System;

namespace SimpleDB.Query
{
    public class Constant : IComparable
    {
        private int? ival = null;
        private String sval = null;
        private DateTime? dateTimeVal = null;

        private Constant()
        {
        }

        public static Constant Null() => new Constant();

        public Constant(int ival)
        {
            this.ival = ival;
        }

        public Constant(String sval)
        {
            this.sval = sval;
        }

        public Constant(DateTime dval)
        {
            this.dateTimeVal = dval;
        }

        public int asInt()
        {
            return ival.Value;
        }

        public String asString()
        {
            return sval;
        }

        public DateTime asDateTime()
        {
            return dateTimeVal.Value;
        }

        public override bool Equals(Object obj)
        {
            Constant c = (Constant)obj;
            return (ival != null) ? ival.Equals(c.ival) : sval.Equals(c.sval);
        }

        public int CompareTo(Object obj)
        {
            return (ival != null) ? ival.Value.CompareTo(((Constant)obj).ival) : sval.CompareTo(((Constant)obj).sval);
        }

        public override int GetHashCode()
        {
            if (ival.HasValue)
                return ival.Value.GetHashCode();

            if (sval != null)
                return sval.GetHashCode();

            if(dateTimeVal.HasValue)
                return dateTimeVal.Value.GetHashCode();

            throw new Exception("unable to calculate GetHashCode");
        }

        public override String ToString()
        {
            if (ival.HasValue)
                return ival.Value.ToString();

            if (sval != null)
                return sval.ToString();

            if (dateTimeVal.HasValue)
                return dateTimeVal.Value.ToString();

            throw new Exception("unable to calculate ToString");
        }

        public bool IsNull()
        {
            return !ival.HasValue && sval == null && !dateTimeVal.HasValue;
        }
    }
}
