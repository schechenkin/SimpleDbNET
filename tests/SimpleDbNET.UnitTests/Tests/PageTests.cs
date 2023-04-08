using FluentAssertions;
using SimpleDb.File;
using SimpleDb.Types;
using Xunit;

namespace SimpleDbNET.UnitTests.Tests;
public class PageTests
{
    [Fact]
    public void SetInt_GetInt()
    {
        var sut = new Page(128);

        sut.SetInt(10, 42);
        sut.GetInt(10).Should().Be(42);
    }

    [Fact]
    public void SetDateTime_GetDateTime()
    {
        var sut = new Page(128);
        var dt = new DateTime(2023, 3, 4);

        sut.SetDateTime(10, dt);
        sut.GetDateTime(10).Should().Be(dt);
    }

    [Fact]
    public void SetString_GetString()
    {
        var sut = new Page(128);
        string str = "hello world";

        sut.SetString(10, str);
        sut.GetString(10).Should().Be(str);
    }

    [Fact]
    public void SetDbString_GetDbString()
    {
        var sut = new Page(128);
        DbString str = "hello world";

        sut.SetValue(10, str);
        sut.GetDbString(10).Should().Be(str);
    }

    [Fact]
    public void SetBit_GetBit()
    {
        var sut = new Page(128);

        sut.SetBit(10, 0, true);
        sut.SetBit(10, 1, false);
        sut.SetBit(10, 2, true);
        sut.SetBit(10, 3, false);

        sut.GetBit(10, 0).Should().Be(true);
        sut.GetBit(10, 1).Should().Be(false);
        sut.GetBit(10, 2).Should().Be(true);
        sut.GetBit(10, 3).Should().Be(false);
    }

    [Fact]
    public void SetBool_GetBool()
    {
        var sut = new Page(128);

        sut.SetValue<bool>(10, true);
        sut.GetBool(10).Should().BeTrue();

        sut.SetValue<bool>(10, false);
        sut.GetBool(10).Should().BeFalse();
    }

    [Fact]
    public void SetBytes_GetBytes()
    {
        var sut = new Page(128);

        sut.SetBytes(10, new byte[] { 1,2,3 });

        sut.GetBytesArray(10).Should().BeEquivalentTo(new byte[] { 1,2,3 });
    }

    [Fact]
    public void SetString_StringCompare()
    {
        var sut = new Page(128);
        string str = "hello world";

        sut.SetString(10, str);
        sut.StringCompare(10, new SimpleDb.Types.DbString(str)).Should().BeTrue();
        sut.StringCompare(10, new SimpleDb.Types.DbString("random")).Should().BeFalse();
    }
}
