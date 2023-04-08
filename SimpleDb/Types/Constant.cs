using System.Diagnostics;
using System;

namespace SimpleDb.Types;
public readonly struct Constant
{
    readonly int _value0;
    readonly DbString _value1;
    readonly DateTime _value2;
    readonly NULL _value3;
    readonly int _index;

    Constant(int index, int value0 = default, DbString value1 = default, DateTime value2 = default, NULL value3 = default)
    {
        _index = index;
        _value0 = value0;
        _value1 = value1;
        _value2 = value2;
        _value3 = value3;
    }

    public static NULL Null()
    {
        return new NULL();
    }

    public int Index => _index;

    public bool IsInt => _index == 0;
    public bool IsString => _index == 1;
    public bool IsDateTime => _index == 2;
    public bool IsNull => _index == 3;

    public int AsInt =>
        _index == 0 ?
            _value0 :
            throw new InvalidOperationException($"Cannot return as int as result is T{_index}");
    public DbString AsString =>
        _index == 1 ?
            _value1 :
            throw new InvalidOperationException($"Cannot return as DbString as result is T{_index}");
    public DateTime AsDateTime =>
        _index == 2 ?
            _value2 :
            throw new InvalidOperationException($"Cannot return as DateTime as result is T{_index}");
    public NULL AsNull =>
        _index == 3 ?
            _value3 :
            throw new InvalidOperationException($"Cannot return as NULL as result is T{_index}");

    public static implicit operator Constant(int t) => new Constant(0, value0: t);
    public static implicit operator Constant(DbString t) => new Constant(1, value1: t);
    public static implicit operator Constant(DateTime t) => new Constant(2, value2: t);
    public static implicit operator Constant(NULL t) => new Constant(3, value3: t);

    public void Switch(Action<int> f0, Action<DbString> f1, Action<DateTime> f2, Action<NULL> f3)
    {
        if (_index == 0 && f0 != null)
        {
            f0(_value0);
            return;
        }
        if (_index == 1 && f1 != null)
        {
            f1(_value1);
            return;
        }
        if (_index == 2 && f2 != null)
        {
            f2(_value2);
            return;
        }
        if (_index == 3 && f3 != null)
        {
            f3(_value3);
            return;
        }
        throw new InvalidOperationException();
    }

    public TResult Match<TResult>(Func<int, TResult> f0, Func<DbString, TResult> f1, Func<DateTime, TResult> f2, Func<NULL, TResult> f3)
    {
        if (_index == 0 && f0 != null)
        {
            return f0(_value0);
        }
        if (_index == 1 && f1 != null)
        {
            return f1(_value1);
        }
        if (_index == 2 && f2 != null)
        {
            return f2(_value2);
        }
        if (_index == 3 && f3 != null)
        {
            return f3(_value3);
        }
        throw new InvalidOperationException();
    }

    /*public static OneOf<int, DbString, DateTime, NULL> Fromint(int input) => input;
    public static OneOf<int, DbString, DateTime, NULL> FromDbString(DbString input) => input;
    public static OneOf<int, DbString, DateTime, NULL> FromDateTime(DateTime input) => input;
    public static OneOf<int, DbString, DateTime, NULL> FromNULL(NULL input) => input;*/
    /*

        public int MapInt<TResult>(Func<int, TResult> mapFunc)
        {
            if (mapFunc == null)
            {
                throw new ArgumentNullException(nameof(mapFunc));
            }
            return _index switch
            {
                0 => mapFunc(AsInt),
                1 => AsString,
                2 => AsDateTime,
                3 => AsNull,
                _ => throw new InvalidOperationException()
            };
        }

        public OneOf<int, TResult, DateTime, NULL> MapString<TResult>(Func<DbString, TResult> mapFunc)
        {
            if (mapFunc == null)
            {
                throw new ArgumentNullException(nameof(mapFunc));
            }
            return _index switch
            {
                0 => AsInt,
                1 => mapFunc(AsString),
                2 => AsDateTime,
                3 => AsNULL,
                _ => throw new InvalidOperationException()
            };
        }

        public OneOf<int, DbString, TResult, NULL> MapDateTime<TResult>(Func<DateTime, TResult> mapFunc)
        {
            if (mapFunc == null)
            {
                throw new ArgumentNullException(nameof(mapFunc));
            }
            return _index switch
            {
                0 => AsInt,
                1 => AsString,
                2 => mapFunc(AsDateTime),
                3 => AsNULL,
                _ => throw new InvalidOperationException()
            };
        }

        public OneOf<int, DbString, DateTime, TResult> MapNull<TResult>(Func<NULL, TResult> mapFunc)
        {
            if (mapFunc == null)
            {
                throw new ArgumentNullException(nameof(mapFunc));
            }
            return _index switch
            {
                0 => AsInt,
                1 => AsString,
                2 => AsDateTime,
                3 => mapFunc(AsNULL),
                _ => throw new InvalidOperationException()
            };
        }*/

    public static bool operator ==(Constant lhs, Constant rhs)
    {
        return lhs.Equals(rhs);
    }

    public static bool operator !=(Constant lhs, Constant rhs) => !(lhs == rhs);

    bool Equals(Constant other) =>
        _index == other._index &&
        _index switch
        {
            0 => Equals(_value0, other._value0),
            1 => _value1.Equals(other._value1),
            2 => Equals(_value2, other._value2),
            3 => Equals(_value3, other._value3),
            _ => false
        };

    public override bool Equals(object? obj)
    {
        Debug.Assert(false, "should not be called");
        return false;
    }

    public override string ToString() =>
        _index switch
        {
            0 => Functions.FormatValue(_value0),
            1 => Functions.FormatValue(_value1),
            2 => Functions.FormatValue(_value2),
            3 => Functions.FormatValue(_value3),
            _ => throw new InvalidOperationException("Unexpected index, which indicates a problem in the OneOf codegen.")
        };

    public override int GetHashCode()
    {
        unchecked
        {
            int hashCode = _index switch
            {
                0 => _value0.GetHashCode(),
                1 => _value1.GetHashCode(),
                2 => _value2.GetHashCode(),
                3 => _value3.GetHashCode(),
                _ => 0
            };
            return (hashCode * 397) ^ _index;
        }
    }

    public bool Equals(in Constant other)
    {
        if (IsInt)
            return _value0 == other._value0;

        if (IsString)
            return _value1.Equals(other._value1);

        if (IsDateTime)
            return _value2 == other._value2;

        return other.IsNull;

    }

    public int CompareTo(in Constant other)
    {
        if (IsInt)
            return _value0.CompareTo(other._value0);

        if (IsString)
            return _value1.GetString().CompareTo(other._value1.GetString());

        if (IsDateTime)
            return _value2.CompareTo(other._value2);

        return 0;
    }
}