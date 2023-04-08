using FluentAssertions;
using SimpleDb.Types;
using Xunit;

namespace SimpleDbNET.UnitTests;

public class ConstantTests
{
    [Fact]
    public void Match()
    {
        Constant intConstant = 42;
        intConstant.Match(intVal => 1, str => 2, dt => 3, Null => 4).Should().Be(1);

        Constant strConstant = new DbString("lol");
        strConstant.Match(intVal => 1, str => 2, dt => 3, Null => 4).Should().Be(2);
    }

    [Fact]
    public void Switch()
    {
        Constant intConstant = 42;
        int res = 0;
        intConstant.Switch(intVal => { res = 1;}, str => {res = 2;}, dt => {res = 3;}, Null => {res = 4;});
        res.Should().Be(1);
    }

    [Fact]
    public void Equals_test()
    {
        Constant val1 = new DbString("lol");
        Constant val2 = new DbString("lol");
        Constant val3 = new DbString("hello");
        
        (val1 == val2).Should().BeTrue();
        (val1 == val3).Should().BeFalse();
    }
}
