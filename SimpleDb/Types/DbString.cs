using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using SimpleDb.File;

namespace SimpleDb.Types;

public struct DbString
{
    string? value;
    Memory<byte> memory;
    bool fromString;
    byte[]? buffer;
    
    public static implicit operator DbString(string str) => new DbString(str);

    public DbString(string str)
    {
        value = str;
        fromString = true;
    }

    public DbString(in Memory<byte> bytes)
    {
        this.memory = bytes;
        fromString = false;
    }

    public int BytesLength()
    {       
        if(value != null)
            return Page.CHARSET.GetByteCount(value);
        else
            return memory.Length;
    }

    public int Length()
    {       
        return GetString().Length;
    }


    public Memory<byte> AsSpan()
    {       
        return memory;
    }

    private Memory<byte> GetMemory()
    {
        if(fromString)
        {
            if(buffer is null)
            {
                Debug.Assert(value != null);
                buffer = Page.CHARSET.GetBytes(value);
                memory = new Memory<byte>(buffer);
            }
        }
        return memory;
    }

    public bool Equals(in DbString other)
    {
        if(fromString && other.fromString)
        {
            Debug.Assert(value != null);
            return value.Equals(other.value);
        }
        else
        {
            if(GetMemory().Length != other.GetMemory().Length)
                return false;

            return GetMemory().Span.SequenceEqual(other.GetMemory().Span);
        }
    }

    public static bool operator ==(in DbString lhs, in DbString rhs)
    {
        return lhs.Equals(rhs);
    }

    public static bool operator !=(in DbString lhs, in DbString rhs) => !(lhs == rhs);

    public string GetString()
    {
        if(value is null)
        {
            value = Page.CHARSET.GetString(memory.Span);
        }

        return value;
    }
}