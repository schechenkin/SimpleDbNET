using FluentAssertions;
using SimpleDB.file;
using Xunit;

namespace SimpleDbNET.UnitTests
{
    public class FileTest
    {
        [Fact]
        public void When_write_at_the_beginning_of_the_file()
        {
            //given
            var fileManager = new FileManager("filetest", 400, new TestBlocksReadWriteTracker());
            BlockId firstBlock = BlockId.New("testfile", 0);
            Page page = new Page(fileManager.BlockSize);

            string importantString = "important string";
            int stringSize = Page.CalculateStringStoringSize(importantString);

            //when
            page.SetString(0, importantString);
            page.SetInt(0 + stringSize, 42);
            page.SetBit(0 + stringSize + 4, 2, true);
            page.SetBit(0 + stringSize + 4, 3, false);
            page.SetBit(0 + stringSize + 4, 4, true);
            fileManager.WritePage(page, firstBlock);

            //then
            Page page2 = new Page(fileManager.BlockSize);
            BlockId block = BlockId.New("testfile", 0);
            fileManager.ReadBlock(block, page2);

            page2.GetString(0).Should().Be(importantString);
            page2.GetInt(0 + stringSize).Should().Be(42);
            page2.GetBit(0 + stringSize + 4, 2).Should().Be(true);
            page2.GetBit(0 + stringSize + 4, 3).Should().Be(false);
            page2.GetBit(0 + stringSize + 4, 4).Should().Be(true);

            page2.StringCompare(0, new StringConstant(importantString)).Should().BeTrue();
            page2.StringCompare(0, new StringConstant("lol")).Should().BeFalse();

        }
    }
}
