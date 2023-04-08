namespace SimpleDb.Buffers;

    public class BufferAbortException : Exception {
        public BufferAbortException() : base()
        {
        }

        public BufferAbortException(string message) : base(message)
        {
        }

        public BufferAbortException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }