using FluentAssertions;
using SimpleDB.Data;
using SimpleDB.file;
using SimpleDB.log;
using Xunit;
using Buffer = SimpleDB.Data.Buffer;

namespace SimpleDbNET.UnitTests
{
    public class BufferTest
    {
        [Fact]
        public void Buffers_test()
        {
            var fileManager = new FileManager("buffertest", 400, new TestBlocksReadWriteTracker(), true);
            var logManager = new LogManager(fileManager, "log");
            BufferManager bm = new BufferManager(fileManager, logManager, 3);

            Buffer buff1 = bm.PinBlock(BlockId.New("testfile", 1));
            buff1.UsageCount.Should().Be(1);
            Page p = buff1.Page;
            int n = p.GetInt(80);
            bool bitValue = p.GetBit(81, 2);
            p.SetInt(80, n + 1);
            p.SetBit(84, 2, true);
            buff1.SetModified(1, 0); //placeholder values
            Console.WriteLine("The new value is " + (n + 1));
            bm.UnpinBuffer(buff1);
            // One of these pins will flush buff1 to disk:
            Buffer buff2 = bm.PinBlock(BlockId.New("testfile", 2));
            Buffer buff3 = bm.PinBlock(BlockId.New("testfile", 3));
            Buffer buff4 = bm.PinBlock(BlockId.New("testfile", 4));

            bm.UnpinBuffer(buff2);
            buff2.IsPinned.Should().BeFalse();
            buff2 = bm.PinBlock(BlockId.New("testfile", 1));
            Page p2 = buff2.Page;

            p.GetInt(80).Should().Be(n + 1);
            p.GetBit(84, 2).Should().Be(true);

            p2.SetInt(80, 9999);     // This modification
            buff2.SetModified(1, 0); // won't get written to disk.
        }

        //[Fact]
        public void BufferMgrTest()
        {
            var fileManager = new FileManager("buffermngtest", 400, new TestBlocksReadWriteTracker(), true);
            var logManager = new LogManager(fileManager, "log");
            BufferManager bm = new BufferManager(fileManager, logManager, 3);

            Buffer[] buff = new Buffer[6];
            buff[0] = bm.PinBlock(BlockId.New("testfile", 0));
            buff[1] = bm.PinBlock(BlockId.New("testfile", 1));
            buff[2] = bm.PinBlock(BlockId.New("testfile", 2));

            bm.UnpinBuffer(buff[1]);
            buff[1] = null;

            buff[3] = bm.PinBlock(BlockId.New("testfile", 0)); // block 0 pinned twice
            buff[4] = bm.PinBlock(BlockId.New("testfile", 1)); // block 1 repinned
            try
            {
                Console.WriteLine("Attempting to pin block 3...");
                buff[5] = bm.PinBlock(BlockId.New("testfile", 3)); // will not work; no buffers left
            }
            catch (BufferAbortException e)
            {
                Console.WriteLine("Exception: No available buffers\n");
            }
            bm.UnpinBuffer(buff[2]); buff[2] = null;
            buff[5] = bm.PinBlock(BlockId.New("testfile", 3)); // now this works

            Console.WriteLine("Final Buffer Allocation:");
            for (int i = 0; i < buff.Length; i++)
            {
                Buffer b = buff[i];
                if (b != null)
                    System.Console.WriteLine("buff[" + i + "] pinned to block " + b.BlockId);
            }
        }
    }
}
