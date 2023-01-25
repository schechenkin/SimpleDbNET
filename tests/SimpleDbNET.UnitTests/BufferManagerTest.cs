using FluentAssertions;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using Xunit;

namespace SimpleDbNET.UnitTests
{
    public class BufferManagerTest
    {
        [Fact]
        public void PinUnpinBlockTest()
        {
            var fileManager = new FileManager("PinBlockTest", 400, new TestBlocksReadWriteTracker(), true);
            var logManager = new LogManager(fileManager, "log");
            var bufferManager = new BufferManager(fileManager, logManager, 3);
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
    }
}
