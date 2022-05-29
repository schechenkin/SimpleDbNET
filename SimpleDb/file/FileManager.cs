namespace SimpleDB.file
{
    public class FileManager
    {
        private readonly string dbDirectory;
        private readonly int blocksize;

        private bool isNew;
        private object fileManagerLock = new object();
        private readonly Dictionary<string, FileStream> openFiles = new Dictionary<string, FileStream>();

        public FileManager(string dbDirectory, int blocksize, bool recreate = false)
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

        /// <summary>
        /// Read data from file block into page
        /// </summary>
        /// <param name="blockId"></param>
        /// <param name="page"></param>
        public void ReadBlock(BlockId blockId, Page page)
        {
            lock(fileManagerLock)
            {
                var fileStream = GetFileStream(blockId.FileName);
                fileStream.Seek(blockId.Number * blocksize, SeekOrigin.Begin);
                fileStream.Read(page.GetBuffer(), 0, blocksize);
            }
        }

        /// <summary>
        /// Write page data to file block
        /// </summary>
        /// <param name="page"></param>
        /// <param name="blockId"></param>
        public void WritePage(Page page, BlockId blockId)
        {
            lock (fileManagerLock)
            {
                var f = GetFileStream(blockId.FileName);
                f.Seek(blockId.Number * blocksize, SeekOrigin.Begin);
                f.Write(page.GetBuffer(), 0, blocksize);
                f.Flush(true);
            }
        }

        public BlockId AppendNewBlock(string filename)
        {
            lock (fileManagerLock)
            {
                int newBlockNumber = (int)GetBlocksCount(filename);
                var blockId = BlockId.New(filename, newBlockNumber);
                byte[] bytes = new byte[blocksize];

                var fileStream = GetFileStream(blockId.FileName);
                fileStream.Seek(blockId.Number * blocksize, SeekOrigin.Begin);
                fileStream.Write(bytes);
                fileStream.Flush(true);
                return blockId;
            }
        }

        public int GetBlocksCount(String filename)
        {
            var f = GetFileStream(filename);
            return (int)(f.Length / blocksize);
        }

        public bool IsNew()
        {
            return isNew;
        }

        public int BlockSize => blocksize;

        public void CloseFiles()
        {
            foreach(var file in openFiles)
            {
                file.Value.Close();
            }
        }

        private FileStream GetFileStream(string filename)
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
