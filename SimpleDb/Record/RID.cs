using System;

namespace SimpleDB.Record
{
    public class RID
    {
		private int _blknum;
		private int _slot;

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

		public override bool Equals(Object obj)
		{
			RID r = (RID)obj;
			return _blknum == r._blknum && _slot == r._slot;
		}

		public override String ToString()
		{
			return "[" + _blknum + ", " + _slot + "]";
		}
	}
}
