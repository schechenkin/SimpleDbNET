using System;
using System.IO;
using System.Text;

namespace SimpleDB.file
{
    public class Page
    {
        private byte[] buffer;
        public static Encoding CHARSET = Encoding.ASCII;

        // For creating data buffers
        public Page(int blocksize)
        {
            buffer = new byte[blocksize];
        }

        // For creating log pages
        public Page(byte[] b)
        {
            buffer = b;
        }

        public int getInt(int offset)
        {
            var stream = new MemoryStream(buffer);
            stream.Position = offset;

            using BinaryReader reader = new BinaryReader(stream, CHARSET, true);

            return reader.ReadInt32();
        }

        public void setInt(int offset, int n)
        {
            var stream = new MemoryStream(buffer);
            stream.Position = offset;

            using BinaryWriter writer = new BinaryWriter(stream, CHARSET, true);
            writer.Write(n);
        }

        public byte[] getBytes(int offset)
        {
            var stream = new MemoryStream(buffer);
            stream.Position = offset;

            using BinaryReader reader = new BinaryReader(stream, CHARSET, true);
            int length = reader.ReadInt32();
            return reader.ReadBytes(length);
        }

        public void setBytes(int offset, byte[] b)
        {
            var stream = new MemoryStream(buffer);
            stream.Position = offset;

            using BinaryWriter writer = new BinaryWriter(stream, CHARSET, true);
            writer.Write(b.Length);
            writer.Write(b);
        }

        public String getString(int offset)
        {
            byte[] b = getBytes(offset);
            return CHARSET.GetString(b);
        }

        public void setString(int offset, String s)
        {
            byte[] b = CHARSET.GetBytes(s);
            setBytes(offset, b);
        }

        public static int maxLength(int strlen)
        {
            //float bytesPerChar = CHARSET.GetMaxByteCount(1);
            float bytesPerChar = 1;
            return sizeof(Int32) + (strlen * (int)bytesPerChar);
        }

        // a package private method, needed by FileMgr
        internal byte[] GetBuffer()
        {
            return buffer;
        }
    }

}
