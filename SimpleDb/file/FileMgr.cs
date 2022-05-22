using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace SimpleDB.file
{
    public class FileMgr
    {
        private string dbDirectory;
        private int blocksize;
        private bool isNew;
        private Mutex mutex = new Mutex();
        private Dictionary<string, FileStream> openFiles = new Dictionary<string, FileStream>();
        public FileMgr(string dbDirectory, int blocksize, bool recreate = false)
        {
            this.dbDirectory = dbDirectory;
            this.blocksize = blocksize;

            if(recreate && Directory.Exists(dbDirectory))
            {
                Directory.Delete(dbDirectory, true);
            }

            isNew = !Directory.Exists(dbDirectory);

            // create the directory if the database is new
            if (isNew)
                Directory.CreateDirectory(dbDirectory);

            // remove any leftover temporary tables
            foreach (var fileName in Directory.GetFiles(dbDirectory))
                if (fileName.StartsWith("temp"))
                    Directory.Delete(Path.Combine(dbDirectory, fileName));
        }

        public void read(BlockId blk, Page p)
        {
            lock(mutex)
            {
                var f = getFile(blk.fileName());
                f.Seek(blk.number() * blocksize, SeekOrigin.Begin);
                f.Read(p.GetBuffer(), 0, blocksize);
            }
        }

        public void write(BlockId blk, Page p)
        {
            lock (mutex)
            {
                var f = getFile(blk.fileName());
                f.Seek(blk.number() * blocksize, SeekOrigin.Begin);
                f.Write(p.GetBuffer(), 0, blocksize);
                f.Flush(true);
            }
        }

        public BlockId append(String filename)
        {
            lock (mutex)
            {
                int newblknum = length(filename);
                BlockId blk = new BlockId(filename, newblknum);
                byte[] b = new byte[blocksize];

                var f = getFile(blk.fileName());
                f.Seek(blk.number() * blocksize, SeekOrigin.Begin);
                f.Write(b);
                f.Flush(true);
                return blk;
            }
        }

        public int length(String filename)
        {
            var f = getFile(filename);
            return (int)(f.Length / blocksize);
        }

        public bool IsNew()
        {
            return isNew;
        }

        public int blockSize()
        {
            return blocksize;
        }

        public void closeFiles()
        {
            foreach(var file in openFiles)
            {
                file.Value.Close();
            }
        }

        private FileStream getFile(String filename)
        {
           if(!openFiles.ContainsKey(filename))
           {
                var fileStream = new FileStream(Path.Combine(dbDirectory, filename), FileMode.OpenOrCreate);
                openFiles.Add(filename, fileStream);
           }

            return openFiles[filename];
       }
    }
}
