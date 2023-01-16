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

        public static ConstantRefStruct Null() => new ConstantRefStruct();

        public bool IsNull() => ival == null;

        public ConstantRefStruct(int ival)
        {
            this.ival = ival;
        }

        public ConstantRefStruct(string sval)
        {

        }

        public ConstantRefStruct()
        {

        }

        public int asInt()
        {
            return ival.Value;
        }

        public bool Equals(ConstantRefStruct other)
        {
            return ival == other.ival;
        }

        public int CompareTo(ConstantRefStruct other)
        {
            //return (ival != null) ? ival.Value.CompareTo(((Constant)obj).ival) : sval.CompareTo(((Constant)obj).sval);
            return (ival != null) ? ival.Value.CompareTo(other.ival) : 0;
        }
    }
}
