﻿using SimpleDb.Record;
using SimpleDb.Transactions;
using SimpleDb.Types;

namespace SimpleDb.Indexes.Hash
{
    class HashIndex : Index
    {
		public static int NUM_BUCKETS = 100;
		private Transaction tx;
		private String idxname;
		private Layout layout;
		private Constant searchkey = Constant.Null();
		private TableScan ts = null;

		/**
		 * Opens a hash index for the specified index.
		 * @param idxname the name of the index
		 * @param sch the schema of the index records
		 * @param tx the calling transaction
		 */
		public HashIndex(Transaction tx, String idxname, Layout layout)
		{
			this.tx = tx;
			this.idxname = idxname;
			this.layout = layout;
		}

		/**
		 * Positions the index before the first index record
		 * having the specified search key.
		 * The method hashes the search key to determine the bucket,
		 * and then opens a table scan on the file
		 * corresponding to the bucket.
		 * The table scan for the previous bucket (if any) is closed.
		 * @see simpledb.index.Index#beforeFirst(simpledb.query.Constant)
		 */
		public void beforeFirst(Constant searchkey)
		{
			close();
			this.searchkey = searchkey;
			int bucket = searchkey.GetHashCode() % NUM_BUCKETS;
			String tblname = idxname + bucket;
			ts = new TableScan(tx, tblname, layout);
		}

		/**
		 * Moves to the next record having the search key.
		 * The method loops through the table scan for the bucket,
		 * looking for a matching record, and returning false
		 * if there are no more such records.
		 * @see simpledb.index.Index#next()
		 */
		public bool next()
		{
			while (ts.Next())
				if (ts.GetValue("dataval").Equals(searchkey))
					return true;
			return false;
		}

		/**
		 * Retrieves the dataRID from the current record
		 * in the table scan for the bucket.
		 * @see simpledb.index.Index#getDataRid()
		 */
		public RID getDataRid()
		{
			int blknum = ts.GetInt("block");
			int id = ts.GetInt("id");
			return new RID(blknum, id);
		}

		/**
		 * Inserts a new record into the table scan for the bucket.
		 * @see simpledb.index.Index#insert(simpledb.query.Constant, simpledb.record.RID)
		 */
		public void insert(Constant val, RID rid)
		{
			beforeFirst(val);
			ts.Insert();
			ts.SetValue("block", (int)rid.blockNumber());
			ts.SetValue("id", rid.slot());
			ts.SetValue("dataval", val);
		}

		/**
		 * Deletes the specified record from the table scan for
		 * the bucket.  The method starts at the beginning of the
		 * scan, and loops through the records until the
		 * specified record is found.
		 * @see simpledb.index.Index#delete(simpledb.query.Constant, simpledb.record.RID)
		 */
		public void delete(Constant val, RID rid)
		{
			beforeFirst(val);
			while (next())
				if (getDataRid().Equals(rid))
				{
					ts.Delete();
					return;
				}
		}

		/**
		 * Closes the index by closing the current table scan.
		 * @see simpledb.index.Index#close()
		 */
		public void close()
		{
			if (ts != null)
				ts.Close();
		}

		/**
		 * Returns the cost of searching an index file having the
		 * specified number of blocks.
		 * The method assumes that all buckets are about the
		 * same size, and so the cost is simply the size of
		 * the bucket.
		 * @param numblocks the number of blocks of index records
		 * @param rpb the number of records per block (not used here)
		 * @return the cost of traversing the index
		 */
		public static int searchCost(int numblocks, int rpb)
		{
			return numblocks / HashIndex.NUM_BUCKETS;
		}
	}
}