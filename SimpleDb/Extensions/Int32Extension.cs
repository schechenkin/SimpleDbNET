using System.Diagnostics;

namespace SimpleDb.Extensions
{
    public static class Int32Extension
    {
        /// <summary>
        /// Copies an int to a byte array: Byte order and sift order are inverted.
        /// </summary>
        /// <param name="source">The integer to convert.</param>
        /// <param name="destination">An arbitrary array of bytes.</param>
        /// <param name="offset">Offset into the desination array.</param>
        public static void CopyToByteArray(this int source, byte[] destination, int offset)
        {
            Debug.Assert(destination != null, "Destination array cannot be null");

            // check if there is enough space for all the 4 bytes we will copy
            Debug.Assert(destination.Length >= offset + 4, "Not enough room in the destination array");

            for (int i = 0, j = sizeof(int) - 1; i < sizeof(int); i++, --j)
            {
                destination[offset + i] = (byte)(source >> (8 * j));
            }
        }

        /// <summary>
        /// Copies an int to a to byte array Little Endian: Byte order and sift order are the same.
        /// </summary>
        /// <param name="source">The integer to convert.</param>
        /// <param name="destination">An arbitrary array of bytes.</param>
        /// <param name="offset">Offset into the desination array.</param>
        public static void CopyToByteArrayLE(this int source, byte[] destination, int offset)
        {
            Debug.Assert(destination != null, "Destination array cannot be null");

            // check if there is enough space for all the 4 bytes we will copy
            Debug.Assert(destination.Length >= offset + 4, "Not enough room in the destination array");

            for (int i = 0, j = 0; i < sizeof(int); i++, j++)
            {
                destination[offset + i] = (byte)(source >> (8 * j));
            }
        }

        public static void CopyBitToByteArrayLE(this bool bitValue, byte[] destination, int byteoffset, int bitOffset)
        {
            Debug.Assert(destination != null, "Destination array cannot be null");

            // check if there is enough space for all the 4 bytes we will copy
            Debug.Assert(destination.Length >= byteoffset + 4, "Not enough room in the destination array");
            int byteIndex = byteoffset + bitOffset / 8;
            int offsetInByte = bitOffset % 8;

            byte mask = (byte)(1 << offsetInByte);
            destination[byteIndex] = (byte)(bitValue ? (destination[byteIndex] | mask) : (destination[byteIndex] & ~mask));
        }

        public static void CopyToByteArray(this long source, byte[] destination, int offset)
        {
            Debug.Assert(destination != null, "Destination array cannot be null");

            // check if there is enough space for all the 8 bytes we will copy
            Debug.Assert(destination.Length >= offset + 8, "Not enough room in the destination array");

            for (int i = 0, j = sizeof(int) - 1; i < sizeof(long); i++, --j)
            {
                destination[offset + i] = (byte)(source >> (8 * j));
            }
        }

        public static void CopyToByteArrayLE(this long source, byte[] destination, int offset)
        {
            Debug.Assert(destination != null, "Destination array cannot be null");

            // check if there is enough space for all the 8 bytes we will copy
            Debug.Assert(destination.Length >= offset + 8, "Not enough room in the destination array");

            for (int i = 0, j = 0; i < sizeof(long); i++, j++)
            {
                destination[offset + i] = (byte)(source >> (8 * j));
            }
        }
    }
}
