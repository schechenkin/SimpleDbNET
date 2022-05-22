using System;

namespace SimpleDB.Query
{
    public class Constant : IComparable
    {
        private int? ival = null;
        private String sval = null;

        public Constant(int ival)
        {
            this.ival = ival;
        }

        public Constant(String sval)
        {
            this.sval = sval;
        }

        public int asInt()
        {
            return ival.Value;
        }

        public String asString()
        {
            return sval;
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
            return (ival != null) ? ival.GetHashCode() : sval.GetHashCode();
        }

        public override String ToString()
        {
            return (ival != null) ? ival.ToString() : sval.ToString();
        }
    }
}
