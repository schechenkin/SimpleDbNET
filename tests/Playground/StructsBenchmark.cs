using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace Playground;

[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net70)]
public class StructsBenchmark
{
    private Worker worker = new();
    private string text = "hello";
    
    [Benchmark]
    public C UseSimpleStruct()
    {
        return worker.DoSomeWork(new C(1, text));
    }

    [Benchmark]
    public C UseSimpleStructWithIn()
    {
        return worker.DoSomeWork(new C(1, text));
    }
    
    [Benchmark]
    public A UseReadonlyStruct()
    {
        return worker.DoSomeWork(new A(1, text));
    }

    [Benchmark]
    public A UseReadonlyStructWithIn()
    {
        return worker.DoSomeWork2(new A(1, text));
    }

    [Benchmark]
    public B UseRefReadonlyStruct()
    {
        return worker.DoSomeWork(new B(1, text));
    }
}

public class Worker
{
    public A DoSomeWork(A a)
    {
        return a;
    }

    public A DoSomeWork2(in A a)
    {
        return a;
    }

    public B DoSomeWork(B b)
    {
        return b;
    }

    public C DoSomeWork(C c)
    {
        return c;
    }

    public C DoSomeWork2(in C c)
    {
        return c;
    }
}

public readonly struct A
{
    public readonly int Id;
    public readonly string Text;

    public A(int id, string text)
    {
        Id = id;
        Text = text;
    }
}

public readonly ref struct B
{
    public readonly int Id;
    public readonly string Text;

    public B(int id, string text)
    {
        Id = id;
        Text = text;
    }
}

public struct C
{
    public int Id;
    public string Text;

    public C(int id, string text)
    {
        Id = id;
        Text = text;
    }
}


