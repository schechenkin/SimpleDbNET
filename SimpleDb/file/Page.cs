﻿using SimpleDb.Extensions;
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

        public void SetBit(int offset, int bitLocation, bool value)
        {
            WriteBitToBuffer(offset, bitLocation, value);
        }

        public bool GetBit(int offset, int bitLocation)
        {
            var intValue = BitConverter.ToInt32(buffer, offset);
            int numberRightposition = intValue >> bitLocation;
            return (numberRightposition & 1) == 1;
        }

        public void SetDateTime(int offset, DateTime dt)
        {
            WriteLongToBuffer(offset, dt.Ticks);
        }

        public DateTime GetDateTime(int offset)
        {
            var ticks = BitConverter.ToInt64(buffer, offset);
            return new DateTime(ticks);
        }

        public byte[] GetBytesArray(int offset)
        {
            int length = GetInt(offset);
            byte[] result = new byte[length];
            Buffer.BlockCopy(buffer, offset + sizeof(Int32), result, 0, length);
            return result;
        }

        private ReadOnlySpan<byte> GetStringBytes(int offset)
        {
            int length = GetInt(offset);
            return new ReadOnlySpan<byte>(buffer, offset + sizeof(Int32), length);
        }

        public bool StringCompare(int offset, StringConstant text)
        {
            int length = GetInt(offset);
            if (length != text.Length())
                return false;

            var stringBytes = new ReadOnlySpan<byte>(buffer, offset + sizeof(Int32), length);
            var textBytes = text.AsSpan();

            return stringBytes.SequenceEqual(textBytes);
        }

        public void SetBytes(int offset, byte[] bytes)
        {
            WriteIntToBuffer(offset, bytes.Length);
            Buffer.BlockCopy(bytes, 0, buffer, offset + sizeof(Int32), bytes.Length);
        }

        public string GetString(int offset)
        {
            ReadOnlySpan<byte> bytes = GetStringBytes(offset);
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

        private void WriteLongToBuffer(int offset, long value)
        {
            if (BitConverter.IsLittleEndian)
                value.CopyToByteArrayLE(buffer, offset);
            else
                value.CopyToByteArray(buffer, offset);
        }

        private void WriteBitToBuffer(int offset, int bitLocation, bool value)
        {
            if (BitConverter.IsLittleEndian)
                value.CopyBitToByteArrayLE(buffer, offset, bitLocation);
            else
                throw new NotImplementedException();
        }
    }
}
