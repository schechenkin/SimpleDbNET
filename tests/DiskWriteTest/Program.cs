using System.Diagnostics;

//28ms to disk D
//3 ms to disk C
public class Program
{
    static Random r = new Random();

    public static void Main(string[] args)
    {
        string fileName = "C:\\temp\\filetest.bin";
        int blockSize = 4096;
        long fileSize = 1000 * 1024 * 1024;
        //long fileSize = 10 * 4096;
        byte[] buffer = new byte[blockSize];
        int attemps = 1000;

        Stopwatch stopwatch = new Stopwatch();

        using (var fileStream = new FileStream(fileName, FileMode.OpenOrCreate))
        {
            //warm up
            for (int i = 0; i < 100; i++)
            {
                WriteToFile(GetRandomOffset(0, (fileSize / blockSize) - 1), blockSize, buffer, fileStream);
            }

            stopwatch.Start();
            for(int i = 0; i < attemps; i++)
            {
                WriteToFile(GetRandomOffset(0, (fileSize / blockSize) - 1), blockSize, buffer, fileStream);
            }
            stopwatch.Stop();
        }

        Console.WriteLine("Total Time is {0} ms", stopwatch.ElapsedMilliseconds);
        Console.WriteLine("Average time is {0} ms", stopwatch.ElapsedMilliseconds / attemps);
    }

    private static long GetRandomOffset(long from, long to)
    {
        return r.NextInt64(from, to);
    }

    private static void WriteToFile(long offset, int blockSize, byte[] buffer, FileStream fileStream)
    {
        fileStream.Seek(offset * blockSize, SeekOrigin.Begin);
        fileStream.Write(buffer, 0, blockSize);
        fileStream.Flush(true);
    }
}