using System.Diagnostics;

namespace SimpleDB.file
{
    public class StringConstant
    {
        private readonly string value;
        private readonly byte[] bytes;
        
        public StringConstant(string str)
        {
            value = str;
            bytes = Page.CHARSET.GetBytes(str);
        }

        public int Length()
        {
            return bytes.Length;
        }

        public ReadOnlySpan<byte> AsSpan()
        {
            return bytes.AsSpan();
        }
    }
}
