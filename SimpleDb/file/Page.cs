using System.Text;

namespace SimpleDB.file
{
    public class Page
    {
        private byte[] buffer;
        public readonly static Encoding CHARSET = Encoding.UTF8;

        // For creating data buffers
        public Page(int blockSize)
        {
            buffer = new byte[blockSize];
        }

        // For creating log pages
        public Page(byte[] b)
        {
            buffer = b;
        }

        public int GetInt(int offset)
        {
            var stream = new MemoryStream(buffer);
            stream.Position = offset;

            using BinaryReader reader = new BinaryReader(stream, CHARSET, true);

            return reader.ReadInt32();
        }

        public void SetInt(int offset, int value)
        {
            var stream = new MemoryStream(buffer);
            stream.Position = offset;

            using BinaryWriter writer = new BinaryWriter(stream, CHARSET, true);
            writer.Write(value);
        }

        public byte[] GetBytes(int offset)
        {
            var stream = new MemoryStream(buffer);
            stream.Position = offset;

            using BinaryReader reader = new BinaryReader(stream, CHARSET, true);
            int length = reader.ReadInt32();
            return reader.ReadBytes(length);
        }

        public void SetBytes(int offset, byte[] bytes)
        {
            var stream = new MemoryStream(buffer);
            stream.Position = offset;

            using BinaryWriter writer = new BinaryWriter(stream, CHARSET, true);
            writer.Write(bytes.Length);
            writer.Write(bytes);
        }

        public string GetString(int offset)
        {
            byte[] bytes = GetBytes(offset);
            return CHARSET.GetString(bytes);
        }

        public void SetString(int offset, String value)
        {
            byte[] b = CHARSET.GetBytes(value);
            SetBytes(offset, b);
        }

        public static int CalculateStringStoringSize(string text)
        {
            return sizeof(Int32) + Encoding.UTF8.GetByteCount(text);
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
    }

}
