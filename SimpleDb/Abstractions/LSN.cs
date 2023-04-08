namespace SimpleDb.Abstractions;

public readonly record struct LSN(long Value)
{
    public static implicit operator LSN(long value) => new LSN(value);
    public static implicit operator long(LSN number) => number.Value;
}
