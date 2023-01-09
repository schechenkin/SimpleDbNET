using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDb.Query
{
    public readonly struct ConstantFast
    {
        private readonly int? ival = null;
        private readonly String sval = null;
        private readonly DateTime? dateTimeVal = null;

        public ConstantFast(int ival)
        {
            this.ival = ival;
        }

        public ConstantFast(String sval)
        {
            this.sval = sval;
        }

        public ConstantFast(DateTime dval)
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
            ConstantFast c = (ConstantFast)obj;
            return (ival != null) ? ival.Equals(c.ival) : sval.Equals(c.sval);
        }

        public int CompareTo(Object obj)
        {
            return (ival != null) ? ival.Value.CompareTo(((ConstantFast)obj).ival) : sval.CompareTo(((ConstantFast)obj).sval);
        }

        public override int GetHashCode()
        {
            if (ival.HasValue)
                return ival.Value.GetHashCode();

            if (sval != null)
                return sval.GetHashCode();

            if (dateTimeVal.HasValue)
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
