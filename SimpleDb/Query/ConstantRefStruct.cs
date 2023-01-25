using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDb.Query
{
    public readonly ref struct ConstantRefStruct
    {
        private readonly int? ival = null;
        private readonly string? sval = null;
        private readonly DateTime? dateTimeVal = null;

        public static ConstantRefStruct Null() => new ConstantRefStruct();

        public bool IsNull() => ival == null && sval == null && dateTimeVal == null;

        public ConstantRefStruct(int ival)
        {
            this.ival = ival;
        }

        public ConstantRefStruct(string sval)
        {
            this.sval = sval;
        }

        public ConstantRefStruct(DateTime dval)
        {
            this.dateTimeVal = dval;
        }

        public ConstantRefStruct()
        {

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

        public bool Equals(ConstantRefStruct other)
        {
            if(ival.HasValue)
                return ival == other.ival;

            if (sval != null)
                return sval == other.sval;

            if (dateTimeVal.HasValue)
                return dateTimeVal == other.dateTimeVal;

            return other.IsNull();

        }

        public int CompareTo(ConstantRefStruct other)
        {
            if(ival.HasValue)
                return ival.Value.CompareTo(other.ival);

            if (sval != null)
                return sval.CompareTo(other.sval);

            if (dateTimeVal.HasValue)
                return dateTimeVal.Value.CompareTo(other.dateTimeVal);

            return 0;
        }
    }
}
