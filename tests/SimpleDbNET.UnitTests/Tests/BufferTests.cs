using FluentAssertions;
using NSubstitute;
using SimpleDb.Abstractions;
using SimpleDb.Buffers;
using SimpleDb.File;
using Xunit;

namespace SimpleDbNET.UnitTests.Tests;
public class BufferTests
{
    [Fact]
    public void Pin_UnPin()
    {
        var sut = new SimpleDb.Buffers.Buffer(Substitute.For<IFileManager>(), Substitute.For<ILogManager>());

        sut.IsPinned.Should().BeFalse();
        sut.Pin();
        sut.IsPinned.Should().BeTrue();
        sut.Unpin();
        sut.IsPinned.Should().BeFalse();
    }

    [Fact]
    public void SetModified()
    {
        var sut = new SimpleDb.Buffers.Buffer(Substitute.For<IFileManager>(), Substitute.For<ILogManager>());

        //When
        sut.AssignToBlock(BlockId.New("fileName", 0));

        //Then
        sut.ModifiedByTransaction().Should().BeNull();
        sut.SetModified(42, 44);
        sut.ModifiedByTransaction().Should().Be((TransactionNumber)42);
    }

    [Fact]
    public void When_AssignToBlock_fresh_buffer()
    {
        var fileManager = Substitute.For<IFileManager>();
        var sut = new SimpleDb.Buffers.Buffer(fileManager, Substitute.For<ILogManager>());

        var blockId = BlockId.New("fileName", 0);

        //When
        sut.AssignToBlock(blockId);

        //Then
        sut.BlockId.Should().Be(blockId);
        sut.IsPinned.Should().BeFalse();

        fileManager.Received().ReadPage(blockId, sut.Page);
        fileManager.DidNotReceiveWithAnyArgs().WritePage(default, default);
    }

    [Fact]
    public void When_AssignToBlock_modified_by_other_transaction_buffer()
    {
        var fileManager = Substitute.For<IFileManager>();
        var logManager = Substitute.For<ILogManager>();
        
        //Given
        var sut = new SimpleDb.Buffers.Buffer(fileManager, logManager);
        LSN lsn = 14;

        var blockId = BlockId.New("fileName", 0);
        sut.AssignToBlock(BlockId.New("fileName", 0));
        sut.SetModified(42, lsn);
        fileManager.ClearReceivedCalls();

        //When
        sut.AssignToBlock(BlockId.New("fileName", 1));


        //Then
        fileManager.Received().WritePage(BlockId.New("fileName", 0), sut.Page);
        fileManager.Received().ReadPage(BlockId.New("fileName", 1), sut.Page);
        logManager.Received().Flush(lsn);

        sut.ModifiedByTransaction().Should().BeNull();

    }
}
