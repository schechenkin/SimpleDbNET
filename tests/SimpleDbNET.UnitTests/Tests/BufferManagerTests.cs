using FluentAssertions;
using NSubstitute;
using SimpleDb.Abstractions;
using SimpleDb.Buffers;
using SimpleDb.File;
using Xunit;

namespace SimpleDbNET.UnitTests.Tests;
public class BufferManagerTests
{
    [Fact]
    public void PinUnpinBlockTest()
    {
        var bufferManager = new BufferManager(Substitute.For<IFileManager>(), Substitute.For<ILogManager>(), 3);
        bufferManager.GetFreeBlockCount().Should().Be(3);

        //pin block
        var buffer = bufferManager.PinBlock(BlockId.New("t1", 0));
        buffer.Should().NotBeNull();
        buffer.IsPinned.Should().BeTrue();
        buffer.UsageCount.Should().Be(1);
        buffer.BlockId.Should().BeEquivalentTo(BlockId.New("t1", 0));
        bufferManager.GetFreeBlockCount().Should().Be(2);

        //unpin buffer
        bufferManager.UnpinBuffer(buffer);
        bufferManager.GetFreeBlockCount().Should().Be(2);
        buffer.IsPinned.Should().BeFalse();
        buffer.UsageCount.Should().Be(1);
        buffer.BlockId.Should().BeEquivalentTo(BlockId.New("t1", 0));

    }

    [Fact]
    public void Test()
    {
        var calc = Substitute.For<Calculator>(1);
        calc.Sum(1, 2).Should().Be(3);
        calc.Received().Sum(1, 2);
    }

    public class Calculator
    {
        public Calculator(int x)
        {

        }

        public int Sum(int a, int b)
        {
            return a + b;
        }
    }

}
