using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.Data
{

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
}
