namespace SimpleDb.File;

    internal class DbFile
    {
        private FileStream _stream;
        private long _length;

        public DbFile(string path)
        {
            _stream = new FileStream(path, FileMode.OpenOrCreate);
            _length = _stream.Length;
        }

        public FileStream Stream => _stream;
        public long Length => _length;

        internal void RecalculateLength()
        {
            _length = _stream.Length;
        }
    }
