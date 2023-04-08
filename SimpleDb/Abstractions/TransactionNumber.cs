namespace SimpleDb.Abstractions;

public readonly record struct TransactionNumber(long Value)
{
    public static implicit operator TransactionNumber(long value) => new TransactionNumber(value);
    public static implicit operator long(TransactionNumber number) => number.Value;

    public static int Size() => sizeof(long);
}
