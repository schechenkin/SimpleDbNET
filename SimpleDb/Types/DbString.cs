using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using SimpleDb.File;

namespace SimpleDb.Types;

public struct DbString
{
    private readonly string value;
    private byte[]? bytes;
    public static implicit operator DbString(string str) => new DbString(str);

    public DbString(string str)
    {
        value = str;
    }

    public int BytesLength()
    {       
        if(bytes is null)
            return Page.CHARSET.GetByteCount(value);
        else
            return bytes.Length;
    }

    public int Length()
    {       
        return value.Length;
    }


    public ReadOnlySpan<byte> AsSpan()
    {
        if(bytes is null)
        {
            bytes = Page.CHARSET.GetBytes(value);
        }
        
        return bytes.AsSpan();
    }

    public byte[] GetBytes()
    {
        if(bytes is null)
            bytes = Page.CHARSET.GetBytes(value);

        return bytes;
    }

    public string GetString() => value;
}