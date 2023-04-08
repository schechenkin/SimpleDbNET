using System;

namespace SimpleDb.Record
{
    public readonly ref struct RID
    {
		private readonly int _blknum;
		private readonly int _slot;

		/**
		 * Create a RID for the record having the
		 * specified location in the specified block.
		 * @param blknum the block number where the record lives
		 * @param slot the record's loction
		 */
		public RID(int blknum, int slot)
		{
			this._blknum = blknum;
			this._slot = slot;
		}

		/**
		 * Return the block number associated with this RID.
		 * @return the block number
		 */
		public int blockNumber()
		{
			return _blknum;
		}

		/**
		 * Return the slot associated with this RID.
		 * @return the slot
		 */
		public int slot()
		{
			return _slot;
		}

		public bool Equals(in RID other) => _blknum == other._blknum && _slot == other._slot;

		public override String ToString()
		{
			return "[" + _blknum + ", " + _slot + "]";
		}
	}
}
