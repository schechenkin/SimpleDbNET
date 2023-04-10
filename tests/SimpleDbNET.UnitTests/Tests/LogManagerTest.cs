using FluentAssertions;
using NSubstitute;
using SimpleDb.Abstractions;
using SimpleDb.Buffers;
using SimpleDb.File;
using SimpleDb.Log;
using Xunit;

namespace SimpleDbNET.UnitTests.Tests;

public class LogManagerTest
{
    [Fact]
    public void Should_return_SLN_when_add_record()
    {
        //given
        var fileManager = new FileManager("loglsntest", 400, true);
        var logManager = new LogManager(fileManager, "log");

        //when
        logManager.Append(new byte[100]).Should().Be((LSN)1);
        logManager.Append(new byte[200]).Should().Be((LSN)2);
        var lsn = logManager.Append(new byte[250]);

        //then
        lsn.Should().Be((LSN)3);

        logManager.CurrentBlockId.Number.Should().Be(1);
    }

    [Fact]
    public void When_iterate_via_reverse_iterator()
    {
        //given
        var fileManager = new FileManager("logtestreverse", 400, true);
        var logManager = new LogManager(fileManager, "log");

        logManager.GetReverseIterator().HasNext().Should().BeFalse();

        //when
        addRecordsToLog(1, 35, logManager);

        logManager.CurrentBlockId.Number.Should().Be(2);

        //then
        int counter = 35;
        foreach (byte[] rec in logManager.GetReverseIterator())
        {
            var record = Record.FromBytes(rec);
            record.Id.Should().Be(counter);
            record.Text.Should().Be($"Text {counter}");

            counter--;
        }

        counter.Should().Be(0);
    }

    [Fact]
    public void When_iterate_log()
    {
        //given
        var fileManager = new FileManager("logtest", 400, true);
        var logManager = new LogManager(fileManager, "log");

        logManager.GetIterator().HasNext().Should().BeFalse();

        //when
        addRecordsToLog(1, 35, logManager);

        logManager.CurrentBlockId.Number.Should().Be(2);

        //then
        int counter = 1;
        foreach (byte[] rec in logManager.GetIterator())
        {
            var record = Record.FromBytes(rec);
            record.Id.Should().Be(counter);
            record.Text.Should().Be($"Text {counter}");

            counter++;
        }

        counter.Should().Be(36);
    }

    private void addRecordsToLog(int start, int end, LogManager logManager)
    {
        for (int i = start; i <= end; i++)
        {
            byte[] bytes = new Record { Id = i, Text = $"Text {i}" }.GetBytes();
            LSN lsn = logManager.Append(bytes);
        }
    }
    public class Record
    {
        public int Id { get; set; }
        public string Text { get; set; } = "";

        public byte[] GetBytes()
        {
            byte[] b = new byte[GetBytesCountToStore()];
            Page p = new Page(b);
            p.SetInt(0, Id);
            p.SetString(sizeof(int), Text);
            return b;
        }

        public int GetBytesCountToStore()
        {
            return Page.CalculateStringStoringSize(Text) + sizeof(int);
        }

        public static Record FromBytes(byte[] bytes)
        {
            Page page = new Page(bytes);
            return new Record { Id = page.GetInt(0), Text = page.GetString(4).GetString() };
        }
    }
}