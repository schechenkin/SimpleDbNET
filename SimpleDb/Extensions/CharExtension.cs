using System.Diagnostics;

namespace SimpleDb.Extensions
{
    public static class CharExtension
    {
        /// <summary>
        /// Copies an int to a byte array: Byte order and sift order are inverted.
        /// </summary>
        /// <param name="source">The integer to convert.</param>
        /// <param name="destination">An arbitrary array of bytes.</param>
        /// <param name="offset">Offset into the desination array.</param>
        public static void CopyToByteArray(this char source, byte[] destination, int offset)
        {
            Debug.Assert(destination != null, "Destination array cannot be null");

            // check if there is enough space for all the 4 bytes we will copy
            Debug.Assert(destination.Length >= offset + sizeof(char), "Not enough room in the destination array");

            
        }

        /// <summary>
        /// Copies an int to a to byte array Little Endian: Byte order and sift order are the same.
        /// </summary>
        /// <param name="source">The integer to convert.</param>
        /// <param name="destination">An arbitrary array of bytes.</param>
        /// <param name="offset">Offset into the desination array.</param>
        public static void CopyToByteArrayLE(this char source, byte[] destination, int offset)
        {
            Debug.Assert(destination != null, "Destination array cannot be null");

            // check if there is enough space for all the 4 bytes we will copy
            Debug.Assert(destination.Length >= offset + sizeof(char), "Not enough room in the destination array");
        }
    }
}
