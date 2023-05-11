using System.ComponentModel.DataAnnotations;
using SimpleDb.Abstractions;
using SimpleDb.Extensions;
using SimpleDb.Types;
using System.Text;

namespace SimpleDb.File;

public readonly struct Page
{
    private readonly byte[] buffer;
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

    public long GetLong(int offset)
    {
        return BitConverter.ToInt64(buffer, offset);
    }

    public bool GetBool(int offset)
    {
        return BitConverter.ToBoolean(buffer, offset);
    }

    public TransactionNumber GetTransactionNumber(int offset)
    {
        return BitConverter.ToInt64(buffer, offset);
    }

    public void SetValue<T>(int offset, T value)
    {
        switch (value)
        {
            case int intVal when value is int:
                SetInt(offset, intVal);
                break;

            case string str when value is string:
                //throw new NotImplementedException("string type not supported here. Use DbString");
                SetString(offset, (DbString)str);
                break;

            case DbString dbstr when value is DbString:
                SetString(offset, dbstr);
                break;

            case DateTime dt when value is DateTime:
                SetDateTime(offset, dt);
                break;

            case TransactionNumber txNum when value is TransactionNumber:
                WriteLongToBuffer(offset, txNum);
                break;

            case long longvalue when value is long:
                WriteLongToBuffer(offset, longvalue);
                break;

            case bool boolvalue when value is bool:
                WriteBoolToBuffer(offset, boolvalue);
                break;

            default:
                throw new NotImplementedException();
        }
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
        byte[] result = new byte[length];//TODO use byte pool
        Buffer.BlockCopy(buffer, offset + sizeof(Int32), result, 0, length);
        return result;
    }

    public void SetBytes(int offset, byte[] bytes)
    {
        WriteIntToBuffer(offset, bytes.Length);
        Buffer.BlockCopy(bytes, 0, buffer, offset + sizeof(Int32), bytes.Length);
    }

    public DbString GetString(int offset)
    {
        int length = GetInt(offset);
        Memory<byte> bytes = new Memory<byte>(buffer, offset + sizeof(Int32), length);
        return new DbString(bytes);
    }

    public void SetString(int offset, in DbString value)
    {
        WriteIntToBuffer(offset, sizeof(char) * value.StringLength());
        WriteSpanToBuffer(offset + sizeof(Int32), value.GetMemory());
    }


    public static int CalculateStringStoringSize(in DbString dbString)
    {
        return sizeof(Int32) + dbString.BytesLength();
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

    private void WriteBoolToBuffer(int offset, bool value)
    {
        if (BitConverter.IsLittleEndian)
            value.CopyToByteArrayLE(buffer, offset);
        else
            throw new NotImplementedException();
    }

    private void WriteSpanToBuffer(int offset, Memory<byte> value)
    {
        if (BitConverter.IsLittleEndian)
            value.CopyToByteArrayLE(buffer, offset);
        else
            throw new NotImplementedException();
    }

    private void WriteBitToBuffer(int offset, int bitLocation, bool value)
    {
        if (BitConverter.IsLittleEndian)
            value.CopyBitToByteArrayLE(buffer, offset, bitLocation);
        else
            throw new NotImplementedException();
    }
}