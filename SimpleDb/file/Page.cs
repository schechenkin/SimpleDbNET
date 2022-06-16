using SimpleDb.Extensions;
using System.Text;

namespace SimpleDB.file
{
    public class Page
    {
        private byte[] buffer;
        public readonly static Encoding CHARSET = Encoding.Unicode;

        // For creating data buffers
        public Page(int blockSize)
        {
            buffer = new byte[blockSize];
        }

        // For creating log pages
        public Page(byte[] bytes)
        {
            buffer = bytes;
        }

        public int GetInt(int offset)
        {
            return BitConverter.ToInt32(buffer, offset);
        }

        public void SetInt(int offset, int value)
        {
            WriteIntToBuffer(offset, value);
        }

        public byte[] GetBytesArray(int offset)
        {
            int length = GetInt(offset);
            byte[] result = new byte[length];
            Buffer.BlockCopy(buffer, offset + sizeof(Int32), result, 0, length);
            return result;
        }

        public ReadOnlySpan<byte> GetBytes(int offset)
        {
            int length = GetInt(offset);
            return new ReadOnlySpan<byte>(buffer, offset + sizeof(Int32), length);
        }

        public void SetBytes(int offset, byte[] bytes)
        {
            WriteIntToBuffer(offset, bytes.Length);
            Buffer.BlockCopy(bytes, 0, buffer, offset + sizeof(Int32), bytes.Length);
        }

        public string GetString(int offset)
        {
            ReadOnlySpan<byte> bytes = GetBytes(offset);
            return CHARSET.GetString(bytes);
        }

        public void SetString(int offset, String value)
        {
            WriteIntToBuffer(offset, sizeof(char) * value.Length);
            CHARSET.GetBytes(value, 0, value.Length, buffer, offset + sizeof(Int32));
        }

        public static int CalculateStringStoringSize(string text)
        {
            return sizeof(Int32) + CHARSET.GetByteCount(text);
        }

        public static int maxLength(int strlen)
        {
            float bytesPerChar = 4;
            return sizeof(Int32) + (strlen * (int)bytesPerChar);
        }

        // a package private method, needed by FileMgr
        internal byte[] GetBuffer()
        {
            return buffer;
        }

        private void WriteIntToBuffer(int offset, int value)
        {
            if (BitConverter.IsLittleEndian)
                value.CopyToByteArrayLE(buffer, offset);
            else
                value.CopyToByteArray(buffer, offset);
        }

        private void WriteCharToBuffer(int offset, char value)
        {
            if (BitConverter.IsLittleEndian)
                value.CopyToByteArrayLE(buffer, offset);
            else
                value.CopyToByteArray(buffer, offset);
        }
    }
}
