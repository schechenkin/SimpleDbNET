﻿using SimpleDb.file;

namespace SimpleDB.file
{
    public class FileManager
    {
        private readonly string dbDirectory;
        private readonly int blocksize;
        private readonly IBlocksReadWriteTracker blocksReadWriteTracker;
        private readonly int blocksPerFile;

        private bool isNew;
        private readonly Dictionary<string, List<DbFile>> openFiles = new Dictionary<string, List<DbFile>>();

        public FileManager(string dbDirectory, int blocksize, IBlocksReadWriteTracker blocksReadWriteTracker, bool recreate = false, int blocksPerFile = 1024)
        {
            this.dbDirectory = dbDirectory;
            this.blocksize = blocksize;
            this.blocksPerFile = blocksPerFile;
            this.blocksReadWriteTracker = blocksReadWriteTracker;

            if (recreate && Directory.Exists(dbDirectory))
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

            foreach(var tableFilePath in Directory.GetFiles(dbDirectory, "*.tbl"))
            {
                var tableFileName = Path.GetFileName(tableFilePath);
                openFiles.Add(tableFileName, new List<DbFile>());
                openFiles[tableFileName].Add(new DbFile(Path.Combine(dbDirectory, tableFileName)));

                var tableName = Path.GetFileNameWithoutExtension(tableFileName);
                foreach(var chunkFilePath in Directory.GetFiles(dbDirectory, $"{tableName}.tbl_*"))
                {
                    openFiles[tableFileName].Add(new DbFile(chunkFilePath));
                }
            }

        }

        /// <summary>
        /// Read data from file block into page
        /// </summary>
        /// <param name="blockId"></param>
        /// <param name="page"></param>
        public void ReadBlock(BlockId blockId, Page page)
        {
            var dbFile = GetDbFile(blockId.FileName, blockId.Number);
            lock (dbFile.Stream)
            {
                dbFile.Stream.Seek((blockId.Number % blocksPerFile) * blocksize, SeekOrigin.Begin);
                dbFile.Stream.Read(page.GetBuffer(), 0, blocksize);
                blocksReadWriteTracker.TrackBlockRead();
            }
        }

        /// <summary>
        /// Write page data to file block
        /// </summary>
        /// <param name="page"></param>
        /// <param name="blockId"></param>
        public void WritePage(Page page, BlockId blockId)
        {
            var dbFile = GetDbFile(blockId.FileName, blockId.Number);
            lock (dbFile.Stream)
            {
                dbFile.Stream.Seek((blockId.Number % blocksPerFile) * blocksize, SeekOrigin.Begin);
                dbFile.Stream.Write(page.GetBuffer(), 0, blocksize);
                dbFile.Stream.Flush(true);
                blocksReadWriteTracker.TrackBlockWrite();
            }
        }

        public BlockId AppendNewBlock(string filename)
        {
            int newBlockNumber = GetBlocksCount(filename);
            var blockId = BlockId.New(filename, newBlockNumber);
            byte[] bytes = new byte[blocksize];

            var dbFile = GetDbFile(filename, newBlockNumber);
            lock (dbFile.Stream)
            {
                dbFile.Stream.Seek((blockId.Number % blocksPerFile) * blocksize, SeekOrigin.Begin);
                dbFile.Stream.Write(bytes);
                dbFile.RecalculateLength();
                dbFile.Stream.Flush(true);
                return blockId;
            }
        }

        public int GetBlocksCount(String filename)
        {
            if (!openFiles.ContainsKey(filename))
                return 0;
            
            var fileChunks = openFiles[filename];
            lock(fileChunks)
            {
                return fileChunks.Sum(fileStream => (int)(fileStream.Length / blocksize));
            }
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
                foreach(DbFile chunk in file.Value)
                {
                    chunk.Stream.Close();
                }
            }

            openFiles.Clear();
        }

        public void ReopenFiles()
        {
            CloseFiles();

            foreach (var tableFilePath in Directory.GetFiles(dbDirectory, "*.tbl"))
            {
                var tableFileName = Path.GetFileName(tableFilePath);
                openFiles.Add(tableFileName, new List<DbFile>());
                openFiles[tableFileName].Add(new DbFile(Path.Combine(dbDirectory, tableFileName)));

                var tableName = Path.GetFileNameWithoutExtension(tableFileName);
                foreach (var chunkFilePath in Directory.GetFiles(dbDirectory, $"{tableName}.tbl_*"))
                {
                    openFiles[tableFileName].Add(new DbFile(chunkFilePath));
                }
            }
        }

        private DbFile GetDbFile(string filename, int blockNumber)
        {
            if (!openFiles.ContainsKey(filename))
            {
                openFiles.Add(filename, new List<DbFile>());
            }

            int part = blockNumber / blocksPerFile;

            var fileChunks = openFiles[filename];
            if(fileChunks.Count < part + 1)
            {
                if (part + 1 - fileChunks.Count > 1)
                    throw new Exception("bad part num");
                
                string chunkFileName = part > 0 ? $"{filename}_{part}" : filename;
                var fileStream = new DbFile(Path.Combine(dbDirectory, chunkFileName));
                lock(fileChunks)
                {
                    fileChunks.Add(fileStream);
                }
            }

           return openFiles[filename][part];
       }
    }
}
