using FluentAssertions;
using SimpleDB.file;
using SimpleDB.log;
using Xunit;

namespace SimpleDbNET.UnitTests
{
    public class LogManagerTest
    {
        
        
        [Fact]
        public void Should_return_SLN_when_add_record()
        {
            //given
            var fileManager = new FileManager("loglsntest", 400, true);
            var logManager = new LogManager(fileManager, "log");

            logManager.GetIterator().HasNext().Should().BeFalse();

            //when
            logManager.Append(new byte[100]);
            logManager.Append(new byte[200]);
            var lsn = logManager.Append(new byte[250]);

            //then
            lsn.Should().Be(3);

            logManager.CurrentBlockId.Number.Should().Be(1);
        }
        
        [Fact]
        public void When_add_and_iterate()
        {
            //given
            var fileManager = new FileManager("logtest", 400, true);
            var logManager = new LogManager(fileManager, "log");

            logManager.GetIterator().HasNext().Should().BeFalse();

            //when
            addRecordsToLog(1, 35, logManager);

            //then
            int counter = 35;
            foreach(byte[] rec in logManager.GetIterator())
            {
                var record = Record.FromBytes(rec);
                record.Id.Should().Be(counter);
                record.Text.Should().Be($"Text {counter}");

                counter--;
            }
        }

        private void addRecordsToLog(int start, int end, LogManager logManager)
        {
            for (int i = start; i <= end; i++)
            {
                byte[] bytes = new Record { Id = i, Text = $"Text {i}" }.GetBytes();
                int lsn = logManager.Append(bytes);
            }
        }
        public class Record
        {
            public int Id { get; set; }
            public string Text { get; set; }

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
                return new Record { Id = page.GetInt(0), Text = page.GetString(4) };
            }
        }
    }
}
