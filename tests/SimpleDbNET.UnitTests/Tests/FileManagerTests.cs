using FluentAssertions;
using SimpleDb.File;
using SimpleDb.Types;
using Xunit;

namespace SimpleDbNET.UnitTests.Tests;
public class FileManagerTests
{
    [Fact]
    public void When_write_at_the_beginning_of_the_file()
    {
        //given
        var fileManager = new FileManager("filetest", 400, true);
        BlockId firstBlock = BlockId.New("testfile", 0);
        Page page = new Page(fileManager.BlockSize);

        string importantString = "important string";
        int stringSize = Page.CalculateStringStoringSize(importantString);
        DateTime dt = new DateTime(2022, 09, 30, 14, 38, 34);

        //when
        page.SetString(0, importantString);
        page.SetInt(0 + stringSize, 42);
        page.SetBit(0 + stringSize + 4, 2, true);
        page.SetBit(0 + stringSize + 4, 3, false);
        page.SetBit(0 + stringSize + 4, 4, true);
        page.SetDateTime(0 + stringSize + 4 + 4, dt);
        fileManager.WritePage(firstBlock, page);

        //then
        Page page2 = new Page(fileManager.BlockSize);
        BlockId block = BlockId.New("testfile", 0);
        fileManager.ReadPage(block, page2);

        page2.GetString(0).Should().Be(importantString);
        page2.GetInt(0 + stringSize).Should().Be(42);
        page2.GetBit(0 + stringSize + 4, 2).Should().Be(true);
        page2.GetBit(0 + stringSize + 4, 3).Should().Be(false);
        page2.GetBit(0 + stringSize + 4, 4).Should().Be(true);

        //page2.StringCompare(0, new DbString(importantString)).Should().BeTrue();
        //page2.StringCompare(0, new DbString("lol")).Should().BeFalse();

        page2.GetDateTime(0 + stringSize + 4 + 4).Should().Be(dt);

    }

    [Fact]
    public void Shrink_file()
    {
        //given
        var fileManager = new FileManager("shrinkfiletest", 400, true);
        BlockId thirdBlock = BlockId.New("testfile", 2);
        Page page = new Page(fileManager.BlockSize);
        page.SetInt(100, 42);
        fileManager.WritePage(thirdBlock, page);
        fileManager.GetBlocksCount("testfile").Should().Be(3);

        //when
        fileManager.Shrink("testfile");

        //then
        fileManager.GetBlocksCount("testfile").Should().Be(0);
    }

    [Fact]
    public void When_write_at_the_2nd_chunk_of_the_file()
    {
        //given
        var fileManager = new FileManager("filetest_chunks", 400, true, 2);
        BlockId firstBlock = BlockId.New("testfile", 0);
        Page page = new Page(fileManager.BlockSize);
        page.SetInt(0, 42);
        fileManager.WritePage(firstBlock, page);

        BlockId block2 = BlockId.New("testfile", 2);
        page = new Page(fileManager.BlockSize);

        string importantString = "important string";

        //when
        page.SetString(0, importantString);
        fileManager.WritePage(block2, page);

        //then
        Page page2 = new Page(fileManager.BlockSize);
        BlockId block = BlockId.New("testfile", 2);
        fileManager.ReadPage(block, page2);

        page2.GetString(0).Should().Be(importantString);

    }
}
