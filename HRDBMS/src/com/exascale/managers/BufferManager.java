package com.exascale.managers;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.managers.FileManager.EndDelayThread;
import com.exascale.misc.VHJOMultiHashMap;
import com.exascale.optimizer.TableScanOperator;
import com.exascale.tables.Schema;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.Read3Thread;
import com.exascale.threads.ReadThread;

public class BufferManager extends HRDBMSThread
{
	public static SubBufferManager[] managers;
	public final static int mLength;
	private final static ConcurrentHashMap<TableScanOperator.ReaderThread, Range> threadRanges = new ConcurrentHashMap<TableScanOperator.ReaderThread, Range>();
	private final static VHJOMultiHashMap<String, Range> fileRanges = new VHJOMultiHashMap<String, Range>();
	private final static ConcurrentHashMap<TableScanOperator.ReaderThread, String> threadFiles = new ConcurrentHashMap<TableScanOperator.ReaderThread, String>();

	static
	{
		mLength = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("num_sbms"));
	}

	public BufferManager(final boolean log)
	{
		managers = new SubBufferManager[mLength];
		int i = 0;
		while (i < mLength)
		{
			managers[i] = new SubBufferManager(log);
			i++;
		}
	}

	public static void flushAll(final FileChannel fc) throws Exception
	{
		final List<FlushThread> threads = new ArrayList<FlushThread>();
		int i = 0;
		while (i < mLength)
		{
			threads.add(new FlushThread(i, fc));
			i += 32;
		}

		for (final FlushThread thread : threads)
		{
			thread.start();
		}

		Exception e = null;
		final Set<String> toForce = new HashSet<String>();
		for (final FlushThread thread : threads)
		{
			thread.join();
			if (thread.getException() != null)
			{
				e = thread.getException();
			}

			toForce.addAll(thread.getToForce());
		}

		final List<EndDelayThread> threads2 = new ArrayList<EndDelayThread>();
		for (final String file : toForce)
		{
			threads2.add(FileManager.endDelay(file));
		}

		boolean allOK = true;
		for (final EndDelayThread thread : threads2)
		{
			thread.join();
			if (!thread.getOK())
			{
				allOK = false;
			}
		}

		if (!allOK)
		{
			throw new IOException();
		}

		if (e != null)
		{
			throw e;
		}
	}

	public static Page getPage(final Block b, final long txnum)
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].getPage(b, txnum);
	}

	public static void invalidateFile(final String fn) throws Exception
	{
		for (final SubBufferManager manager : managers)
		{
			manager.invalidateFile(fn);
		}
	}

	public static boolean isInterest(final Block b)
	{
		if (b == null)
		{
			return false;
		}

		final String fn = b.fileName();
		final int num = b.number();
		final List<Range> ranges = fileRanges.get(fn);
		int z = 0;
		final int limit = ranges.size();
		while (z < limit)
		{
			try
			{
				final Range range = ranges.get(z++);
				if (range != null && num >= range.low && num <= range.high)
				{
					return true;
				}
			}
			catch (final Exception e)
			{
			}
		}

		return false;
	}

	public static ReadThread pin(final Block b, final long txnum) throws Exception
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].pin(b, txnum);
	}

	public static ReadThread pin(final Block b, final long txnum, final List<Integer> cols, final int layoutSize) throws Exception
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].pin(b, txnum, cols, layoutSize);
	}

	public static ReadThread pin(final Block b, final long txnum, final List<Integer> cols, final int layoutSize, final int rank, final int rankSize) throws Exception
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].pin(b, txnum, cols, layoutSize, rank, rankSize);
	}

	public static ReadThread pin(final Block b, final long txnum, final int rank, final int rankSize) throws Exception
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].pin(b, txnum, rank, rankSize);
	}

	public static void pin(final Block b, final Transaction tx, final Schema schema, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos) throws Exception
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].pin(b, tx, schema, schemaMap, fetchPos);
	}

	public static void pin(final Block b, final Transaction tx, final Schema schema, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos, final int rank, final int rankSize) throws Exception
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].pin(b, tx, schema, schemaMap, fetchPos, rank, rankSize);
	}

	public static Read3Thread pin3(final Block b, final long txnum) throws Exception
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].pin3(b, txnum);
	}

	public static Read3Thread pin3(final Block b, final long txnum, final List<Integer> cols, final int layoutSize) throws Exception
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].pin3(b, txnum, cols, layoutSize);
	}

	public static Read3Thread pin3(final Block b, final long txnum, final int rank, final int rankSize) throws Exception
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].pin3(b, txnum, rank, rankSize);
	}

	public static void pin3(final Block b, final Transaction tx, final Schema schema1, final Schema schema2, final Schema schema3, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos) throws Exception
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].pin3(b, tx, schema1, schema2, schema3, schemaMap, fetchPos);
	}

	public static void pin3(final Block b, final Transaction tx, final Schema schema1, final Schema schema2, final Schema schema3, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos, final int rank, final int rankSize) throws Exception
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].pin3(b, tx, schema1, schema2, schema3, schemaMap, fetchPos, rank, rankSize);
	}

	public static ReadThread pinConsecutive(final Block b, final long txnum, final int num, final List<Integer> cols, final int layoutSize, final int rank, final int rankSize) throws Exception
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].requestConsecutivePages(b, txnum, num, rank, rankSize);
	}

	public static void pinSync(final Block b, final long txnum) throws Exception
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].pinSync(b, txnum);
	}

	public static void pinSync(final Block b, final Transaction tx, final Schema schema, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos) throws Exception
	{
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].pinSync(b, tx, schema, schemaMap, fetchPos);
	}

	public static void registerInterest(final TableScanOperator.ReaderThread op, final String fn, final int low, final int high)
	{
		final Range range = new Range(low, high);
		threadRanges.put(op, range);
		fileRanges.multiPut(fn, range);
		threadFiles.put(op, fn);
	}

	public static void request3Pages(final Block[] reqBlocks, final long txnum) throws Exception
	{
		new Request3PagesThread(reqBlocks, txnum).start();
	}

	public static void request3Pages(final Block[] reqBlocks, final Transaction tx, final Schema[] schemas, final int schemaIndex, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos) throws Exception
	{
		new Request3PagesThread2(reqBlocks, tx, schemas, schemaIndex, schemaMap, fetchPos).start();
	}

	public static void requestConsecutivePages(final Block b, final long txnum, final int num)
	{
		try
		{
			final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
			managers[hash].requestConsecutivePages(b, txnum, num, 1, 1);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.warn("Error fetching pages", e);
		}
	}

	public static RequestPagesThread requestConsecutivePages(final Block firstBlock, final long txnum, final int num, final List<Integer> cols, final int layoutSize, final int rank) throws Exception
	{
		final RequestPagesThread retval = new RequestPagesThread(firstBlock, txnum, num, cols, layoutSize, rank);
		retval.start();
		return retval;
	}

	public static void requestPage(final Block b, final long txnum)
	{
		try
		{
			final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
			managers[hash].requestPage(b, txnum);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.warn("Error fetching pages", e);
		}
	}

	public static RequestPagesThread requestPages(final Block[] reqBlocks, final long txnum) throws Exception
	{
		final RequestPagesThread retval = new RequestPagesThread(reqBlocks, txnum);
		retval.start();
		return retval;
	}

	public static RequestPagesThread requestPages(final Block[] reqBlocks, final long txnum, final List<Integer> cols, final int layoutSize) throws Exception
	{
		final RequestPagesThread retval = new RequestPagesThread(reqBlocks, txnum, cols, layoutSize);
		retval.start();
		return retval;
	}

	public static void requestPages(final Block[] reqBlocks, final Transaction tx, final Schema[] schemas, final int schemaIndex, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos) throws Exception
	{
		new RequestPagesThread2(reqBlocks, tx, schemas, schemaIndex, schemaMap, fetchPos).start();
	}

	public static void requestPagesSync(final Block[] reqBlocks, final long txnum) throws Exception
	{
		try
		{
			for (final Block b : reqBlocks)
			{
				BufferManager.pin(b, txnum);
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
		}
	}

	public static void throwAwayPage(final String fn, final int blockNum) throws Exception
	{
		final Block b = new Block(fn, blockNum);
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].throwAwayPage(b);
	}

	public static void unpin(final Page p, final long txnum)
	{
		final int hash = (p.block().fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].unpin(p, txnum);
	}

	public static void unpinAll()
	{
		int i = 0;
		while (i < mLength)
		{
			managers[i].unpinAll();
			i++;
		}
	}

	public static void unpinAll(final long txnum)
	{
		int i = 0;
		while (i < mLength)
		{
			managers[i].unpinAll(txnum);
			i++;
		}
	}

	public static void unpinAllExcept(final long txnum, final String iFn, final Block inUse)
	{
		int i = 0;
		while (i < mLength)
		{
			managers[i].unpinAllExcept(txnum, iFn, inUse);
			i++;
		}
	}

	public static void unpinAllLessThan(final long txnum, final int lt, final String tFn)
	{
		int i = 0;
		while (i < mLength)
		{
			managers[i].unpinAllLessThan(txnum, lt, tFn);
			i++;
		}
	}

	public static void unpinAllLessThan(final long txnum, final int lt, final String tFn, final boolean flag)
	{
		int i = 0;
		while (i < mLength)
		{
			managers[i].unpinAllLessThan(txnum, lt, tFn, flag);
			i++;
		}
	}

	public static void unpinMyDevice(final long txnum, final String prefix)
	{
		int i = 0;
		while (i < mLength)
		{
			managers[i].unpinMyDevice(txnum, prefix);
			i++;
		}
	}

	public static void unregisterInterest(final TableScanOperator.ReaderThread op)
	{
		final Range range = threadRanges.remove(op);
		final String fn = threadFiles.remove(op);
		final List<Range> ranges = fileRanges.get(fn);
		ranges.remove(range);
	}

	public static void updateProgress(final TableScanOperator.ReaderThread op, final int low)
	{
		threadRanges.get(op).low = low;
	}

	public static void write(final Page p, final int off, final byte[] data)
	{
		final int hash = (p.block().fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].write(p, off, data);
	}

	@Override
	public void run()
	{
	}

	public static class FlushThread extends HRDBMSThread
	{
		private final int i;
		private final FileChannel fc;
		private Exception e = null;
		private Set<String> toForce = new HashSet<String>();

		public FlushThread(final int i, final FileChannel fc)
		{
			this.i = i;
			this.fc = fc;
		}

		public Exception getException()
		{
			return e;
		}

		public Set<String> getToForce()
		{
			return toForce;
		}

		@Override
		public void run()
		{
			try
			{
				int j = i;
				toForce = managers[j].flushAll(fc);
				j++;
				while (j < i + 32 && j < mLength)
				{
					toForce.addAll(managers[j].flushAll(fc));
					j++;
				}
			}
			catch (final Exception e)
			{
				this.e = e;
			}
		}
	}

	public static class Range
	{
		public volatile int low;
		public final int high;

		public Range(final int low, final int high)
		{
			this.low = low;
			this.high = high;
		}

		@Override
		public boolean equals(final Object r)
		{
			final Range rhs = (Range)r;
			return low == rhs.low && high == rhs.high;
		}

		@Override
		public int hashCode()
		{
			int hash = 23;
			hash = hash * 31 + low;
			hash = hash * 31 + high;
			return hash;
		}
	}

	public static class RequestPagesThread extends HRDBMSThread
	{
		private Block[] reqBlocks;
		private long txnum;
		private List<Integer> cols;
		private int layoutSize;
		private Block firstBlock;
		private int num = -1;
		private int inRank = -1;
		private List<RequestPagesThread> threads;

		public RequestPagesThread(final List<RequestPagesThread> threads)
		{
			this.threads = threads;
		}

		public RequestPagesThread(final Block firstBlock, final long txnum, final int num, final List<Integer> cols, final int layoutSize, final int inRank)
		{
			this.txnum = txnum;
			this.cols = cols;
			this.layoutSize = layoutSize;
			this.firstBlock = firstBlock;
			this.num = num;
			this.inRank = inRank;
		}

		public RequestPagesThread(final Block[] reqBlocks, final long txnum)
		{
			this.reqBlocks = reqBlocks;
			this.txnum = txnum;
		}

		public RequestPagesThread(final Block[] reqBlocks, final long txnum, final List<Integer> cols, final int layoutSize)
		{
			this.reqBlocks = reqBlocks;
			this.txnum = txnum;
			this.cols = cols;
			this.layoutSize = layoutSize;
		}

		@Override
		public void run()
		{
			try
			{
				if (threads != null)
				{
					for (final RequestPagesThread thread : threads)
					{
						if (thread != null)
						{
							thread.join();
						}
					}

					return;
				}

				if (cols == null)
				{
					try
					{
						final int rankSize = reqBlocks.length;
						int rank = 1;
						for (final Block b : reqBlocks)
						{
							BufferManager.pin(b, txnum, rank, rankSize);
							rank++;
						}

						return;
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
					}
				}

				if (num != -1)
				{
					int rankSize;
					if (inRank + 1 <= 10)
					{
						rankSize = 10;
					}
					else
					{
						rankSize = inRank + 1;
					}

					final ReadThread thread = BufferManager.pinConsecutive(firstBlock, txnum, num, cols, layoutSize, inRank + 1, rankSize);
					thread.join();
					return;
				}

				final List<ReadThread> threads = new ArrayList<ReadThread>();
				final int rankSize = reqBlocks.length;
				try
				{
					int rank = 1;
					for (final Block b : reqBlocks)
					{
						final ReadThread thread = BufferManager.pin(b, txnum, cols, layoutSize, rank, rankSize);

						if (thread != null)
						{
							threads.add(thread);
						}

						rank++;
					}

					for (final ReadThread thread : threads)
					{
						thread.join();
					}

					return;
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
				}
				/*
				 * ArrayList<Read3Thread> threads2 = new
				 * ArrayList<Read3Thread>(); IdentityHashMap<Read3Thread,
				 * ArrayList<Block>> notNeededMap = new
				 * IdentityHashMap<Read3Thread, List<Block>>();
				 *
				 * int prevSBlock = -1; ArrayList<Block> blocks = new
				 * ArrayList<Block>(); for (final Block b : reqBlocks) { int
				 * sBlock = b.number() / 3; if (sBlock != prevSBlock &&
				 * blocks.size() != 0) { if (blocks.size() == 1) { ReadThread
				 * thread = null; thread = BufferManager.pin(blocks.get(0),
				 * txnum, cols, layoutSize);
				 *
				 * if (thread != null) { threads.add(thread); } blocks.clear();
				 * } else { //multi-page Read3Thread thread = null; Block b2 =
				 * blocks.get(0); int page = prevSBlock * 3; Block b3 = new
				 * Block(b2.fileName(), page); Block b4 = new
				 * Block(b2.fileName(), page+1); Block b5 = new
				 * Block(b2.fileName(), page+2);
				 *
				 * Integer numBlocks = FileManager.numBlocks.get(b2.fileName());
				 * if (numBlocks == null) { FileManager.getFile(b2.fileName());
				 * numBlocks = FileManager.numBlocks.get(b2.fileName()); }
				 *
				 * if (page + 2 >= numBlocks) { ReadThread thread2 =
				 * BufferManager.pin(b3, txnum, cols, layoutSize);
				 *
				 * if (thread2 != null) { threads.add(thread2); }
				 *
				 * thread2 = BufferManager.pin(b4, txnum, cols, layoutSize);
				 *
				 * if (thread2 != null) { threads.add(thread2); } } else {
				 * ArrayList<Block> notNeeded = new ArrayList<Block>(); if
				 * (!blocks.contains(b3)) { notNeeded.add(b3); } if
				 * (!blocks.contains(b4)) { notNeeded.add(b4); } if
				 * (!blocks.contains(b5)) { notNeeded.add(b5); }
				 *
				 * thread = BufferManager.pin3(b3, txnum, cols, layoutSize);
				 *
				 * if (thread != null) { threads2.add(thread);
				 * notNeededMap.put(thread, notNeeded); } else { Transaction tx
				 * = new Transaction(txnum); for (Block bl : notNeeded) {
				 * tx.unpin(tx.getPage(bl)); } } }
				 *
				 * blocks.clear(); }
				 *
				 * prevSBlock = sBlock; blocks.add(b); } else if (blocks.size()
				 * != 0) { //same as prev blocks.add(b); } else { prevSBlock =
				 * sBlock; blocks.add(b); } }
				 *
				 * if (blocks.size() == 1) { ReadThread thread = null;
				 *
				 * thread = BufferManager.pin(blocks.get(0), txnum, cols,
				 * layoutSize);
				 *
				 * if (thread != null) { threads.add(thread); } blocks.clear();
				 * } else { //multi-page Read3Thread thread = null; Block b2 =
				 * blocks.get(0); int page = prevSBlock * 3; Block b3 = new
				 * Block(b2.fileName(), page); Block b4 = new
				 * Block(b2.fileName(), page+1); Block b5 = new
				 * Block(b2.fileName(), page+2);
				 *
				 * Integer numBlocks = FileManager.numBlocks.get(b2.fileName());
				 * if (numBlocks == null) { FileManager.getFile(b2.fileName());
				 * numBlocks = FileManager.numBlocks.get(b2.fileName()); }
				 *
				 * if (page + 2 >= numBlocks) { ReadThread thread2 =
				 * BufferManager.pin(b3, txnum, cols, layoutSize);
				 *
				 * if (thread2 != null) { threads.add(thread2); }
				 *
				 * thread2 = BufferManager.pin(b4, txnum, cols, layoutSize);
				 *
				 * if (thread2 != null) { threads.add(thread2); } } else {
				 * ArrayList<Block> notNeeded = new ArrayList<Block>(); if
				 * (!blocks.contains(b3)) { notNeeded.add(b3); } if
				 * (!blocks.contains(b4)) { notNeeded.add(b4); } if
				 * (!blocks.contains(b5)) { notNeeded.add(b5); }
				 *
				 * thread = BufferManager.pin3(b3, txnum, cols, layoutSize);
				 *
				 * if (thread != null) { threads2.add(thread);
				 * notNeededMap.put(thread, notNeeded); } else { Transaction tx
				 * = new Transaction(txnum); for (Block bl : notNeeded) {
				 * tx.unpin(tx.getPage(bl)); } } }
				 *
				 * blocks.clear(); }
				 *
				 * for (ReadThread thread : threads) { thread.join(); }
				 *
				 * for (Read3Thread thread : threads2) { thread.join(); }
				 *
				 * Transaction tx = new Transaction(txnum); for
				 * (List<Block> notNeeded : notNeededMap.values()) { for
				 * (Block bl : notNeeded) { tx.unpin(tx.getPage(bl)); } }
				 */
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}

	private static class Request3PagesThread extends HRDBMSThread
	{
		private final Block[] reqBlocks;
		private final long txnum;

		public Request3PagesThread(final Block[] reqBlocks, final long txnum)
		{
			this.reqBlocks = reqBlocks;
			this.txnum = txnum;
		}

		@Override
		public void run()
		{
			try
			{
				// HRDBMSWorker.logger.debug("Short BM request pages thread
				// starting");
				final int rankSize = reqBlocks.length;
				int rank = 1;
				for (final Block b : reqBlocks)
				{
					BufferManager.pin3(b, txnum, rank, rankSize);
					rank++;
				}
				// HRDBMSWorker.logger.debug("Short BM request pages thread
				// ending");
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}

	private static class Request3PagesThread2 extends HRDBMSThread
	{
		private final Block[] reqBlocks;
		private final Transaction tx;
		private final Schema[] schemas;
		private int schemaIndex;
		private final ConcurrentHashMap<Integer, Schema> schemaMap;
		private final List<Integer> fetchPos;

		public Request3PagesThread2(final Block[] reqBlocks, final Transaction tx, final Schema[] schemas, final int schemaIndex, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos)
		{
			this.reqBlocks = reqBlocks;
			this.tx = tx;
			this.schemas = schemas;
			this.schemaIndex = schemaIndex;
			this.schemaMap = schemaMap;
			this.fetchPos = fetchPos;
		}

		@Override
		public void run()
		{
			try
			{
				// HRDBMSWorker.logger.debug("Long BM request pages thread
				// starting");
				final int rankSize = reqBlocks.length;
				int rank = 1;
				for (final Block b : reqBlocks)
				{
					BufferManager.pin3(b, tx, schemas[schemaIndex], schemas[schemaIndex + 1], schemas[schemaIndex + 2], schemaMap, fetchPos, rank, rankSize);
					schemaIndex += 3;
					rank++;
				}
				// HRDBMSWorker.logger.debug("Long BM request pages thread
				// ending");
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}

	private static class RequestPagesThread2 extends HRDBMSThread
	{
		private final Block[] reqBlocks;
		private final Transaction tx;
		private final Schema[] schemas;
		private int schemaIndex;
		private final ConcurrentHashMap<Integer, Schema> schemaMap;
		private final List<Integer> fetchPos;

		public RequestPagesThread2(final Block[] reqBlocks, final Transaction tx, final Schema[] schemas, final int schemaIndex, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos)
		{
			this.reqBlocks = reqBlocks;
			this.tx = tx;
			this.schemas = schemas;
			this.schemaIndex = schemaIndex;
			this.schemaMap = schemaMap;
			this.fetchPos = fetchPos;
		}

		@Override
		public void run()
		{
			try
			{
				final int rankSize = reqBlocks.length;
				int rank = 1;
				for (final Block b : reqBlocks)
				{
					BufferManager.pin(b, tx, schemas[schemaIndex++], schemaMap, fetchPos, rank, rankSize);
					rank++;
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}
}
