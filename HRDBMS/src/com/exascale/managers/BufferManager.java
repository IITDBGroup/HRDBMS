package com.exascale.managers;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

	public BufferManager(boolean log)
	{
		managers = new SubBufferManager[mLength];
		int i = 0;
		while (i < mLength)
		{
			managers[i] = new SubBufferManager(log);
			i++;
		}
	}

	public static void flushAll(FileChannel fc) throws Exception
	{
		ArrayList<FlushThread> threads = new ArrayList<FlushThread>();
		int i = 0;
		while (i < mLength)
		{
			threads.add(new FlushThread(i, fc));
			i += 32;
		}

		for (FlushThread thread : threads)
		{
			thread.start();
		}

		Exception e = null;
		HashSet<String> toForce = new HashSet<String>();
		for (FlushThread thread : threads)
		{
			thread.join();
			if (thread.getException() != null)
			{
				e = thread.getException();
			}

			toForce.addAll(thread.getToForce());
		}

		ArrayList<EndDelayThread> threads2 = new ArrayList<EndDelayThread>();
		for (String file : toForce)
		{
			threads2.add(FileManager.endDelay(file));
		}

		boolean allOK = true;
		for (EndDelayThread thread : threads2)
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

	public static Page getPage(Block b, long txnum)
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].getPage(b, txnum);
	}

	public static void invalidateFile(String fn) throws Exception
	{
		for (SubBufferManager manager : managers)
		{
			manager.invalidateFile(fn);
		}
	}

	public static boolean isInterest(Block b)
	{
		if (b == null)
		{
			return false;
		}

		String fn = b.fileName();
		int num = b.number();
		List<Range> ranges = fileRanges.get(fn);
		int z = 0;
		final int limit = ranges.size();
		while (z < limit)
		{
			try
			{
				Range range = ranges.get(z++);
				if (range != null && num >= range.low && num <= range.high)
				{
					return true;
				}
			}
			catch (Exception e)
			{
			}
		}

		return false;
	}

	public static ReadThread pin(Block b, long txnum) throws Exception
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].pin(b, txnum);
	}
	
	public static ReadThread pin(Block b, long txnum, int rank, int rankSize) throws Exception
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].pin(b, txnum, rank, rankSize);
	}

	public static ReadThread pin(Block b, long txnum, ArrayList<Integer> cols, int layoutSize) throws Exception
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].pin(b, txnum, cols, layoutSize);
	}
	
	public static ReadThread pin(Block b, long txnum, ArrayList<Integer> cols, int layoutSize, int rank, int rankSize) throws Exception
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].pin(b, txnum, cols, layoutSize, rank, rankSize);
	}
	
	public static ReadThread pinConsecutive(Block b, long txnum, int num, ArrayList<Integer> cols, int layoutSize, int rank, int rankSize) throws Exception
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].requestConsecutivePages(b, txnum, num, rank, rankSize);
	}

	public static void pin(Block b, Transaction tx, Schema schema, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos) throws Exception
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].pin(b, tx, schema, schemaMap, fetchPos);
	}
	
	public static void pin(Block b, Transaction tx, Schema schema, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos, int rank, int rankSize) throws Exception
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].pin(b, tx, schema, schemaMap, fetchPos, rank, rankSize);
	}

	public static Read3Thread pin3(Block b, long txnum) throws Exception
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].pin3(b, txnum);
	}
	
	public static Read3Thread pin3(Block b, long txnum, int rank, int rankSize) throws Exception
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].pin3(b, txnum, rank, rankSize);
	}

	public static Read3Thread pin3(Block b, long txnum, ArrayList<Integer> cols, int layoutSize) throws Exception
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		return managers[hash].pin3(b, txnum, cols, layoutSize);
	}

	public static void pin3(Block b, Transaction tx, Schema schema1, Schema schema2, Schema schema3, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos) throws Exception
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].pin3(b, tx, schema1, schema2, schema3, schemaMap, fetchPos);
	}
	
	public static void pin3(Block b, Transaction tx, Schema schema1, Schema schema2, Schema schema3, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos, int rank, int rankSize) throws Exception
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].pin3(b, tx, schema1, schema2, schema3, schemaMap, fetchPos, rank, rankSize);
	}

	public static void pinSync(Block b, long txnum) throws Exception
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].pinSync(b, txnum);
	}

	public static void pinSync(Block b, Transaction tx, Schema schema, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos) throws Exception
	{
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].pinSync(b, tx, schema, schemaMap, fetchPos);
	}

	public static void registerInterest(TableScanOperator.ReaderThread op, String fn, int low, int high)
	{
		Range range = new Range(low, high);
		threadRanges.put(op, range);
		fileRanges.multiPut(fn, range);
		threadFiles.put(op, fn);
	}

	public static void request3Pages(Block[] reqBlocks, long txnum) throws Exception
	{
		new Request3PagesThread(reqBlocks, txnum).start();
	}

	public static void request3Pages(Block[] reqBlocks, Transaction tx, Schema[] schemas, int schemaIndex, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos) throws Exception
	{
		new Request3PagesThread2(reqBlocks, tx, schemas, schemaIndex, schemaMap, fetchPos).start();
	}

	public static void requestPage(Block b, long txnum)
	{
		try
		{
			int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
			managers[hash].requestPage(b, txnum);
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.warn("Error fetching pages", e);
		}
	}
	
	public static void requestConsecutivePages(Block b, long txnum, int num)
	{
		try
		{
			int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
			managers[hash].requestConsecutivePages(b, txnum, num, 1, 1);
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.warn("Error fetching pages", e);
		}
	}

	public static RequestPagesThread requestPages(Block[] reqBlocks, long txnum) throws Exception
	{
		RequestPagesThread retval = new RequestPagesThread(reqBlocks, txnum);
		retval.start();
		return retval;
	}

	public static RequestPagesThread requestPages(Block[] reqBlocks, long txnum, ArrayList<Integer> cols, int layoutSize) throws Exception
	{
		RequestPagesThread retval = new RequestPagesThread(reqBlocks, txnum, cols, layoutSize);
		retval.start();
		return retval;
	}
	
	public static RequestPagesThread requestConsecutivePages(Block firstBlock, long txnum, int num, ArrayList<Integer> cols, int layoutSize, int rank) throws Exception
	{
		RequestPagesThread retval = new RequestPagesThread(firstBlock, txnum, num, cols, layoutSize, rank);
		retval.start();
		return retval;
	}

	public static void requestPages(Block[] reqBlocks, Transaction tx, Schema[] schemas, int schemaIndex, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos) throws Exception
	{
		new RequestPagesThread2(reqBlocks, tx, schemas, schemaIndex, schemaMap, fetchPos).start();
	}

	public static void requestPagesSync(Block[] reqBlocks, long txnum) throws Exception
	{
		try
		{
			for (final Block b : reqBlocks)
			{
				BufferManager.pin(b, txnum);
			}
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
		}
	}

	public static void throwAwayPage(String fn, int blockNum) throws Exception
	{
		Block b = new Block(fn, blockNum);
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % mLength;
		managers[hash].throwAwayPage(b);
	}

	public static void unpin(Page p, long txnum)
	{
		int hash = (p.block().hashCode2() & 0x7FFFFFFF) % mLength;
		managers[hash].unpin(p, txnum);
	}

	public static void unpinAll(long txnum)
	{
		int i = 0;
		while (i < mLength)
		{
			managers[i].unpinAll(txnum);
			i++;
		}
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

	public static void unpinAllExcept(long txnum, String iFn, Block inUse)
	{
		int i = 0;
		while (i < mLength)
		{
			managers[i].unpinAllExcept(txnum, iFn, inUse);
			i++;
		}
	}

	public static void unpinAllLessThan(long txnum, int lt, String tFn)
	{
		int i = 0;
		while (i < mLength)
		{
			managers[i].unpinAllLessThan(txnum, lt, tFn);
			i++;
		}
	}

	public static void unpinAllLessThan(long txnum, int lt, String tFn, boolean flag)
	{
		int i = 0;
		while (i < mLength)
		{
			managers[i].unpinAllLessThan(txnum, lt, tFn, flag);
			i++;
		}
	}

	public static void unpinMyDevice(long txnum, String prefix)
	{
		int i = 0;
		while (i < mLength)
		{
			managers[i].unpinMyDevice(txnum, prefix);
			i++;
		}
	}

	public static void unregisterInterest(TableScanOperator.ReaderThread op)
	{
		Range range = threadRanges.remove(op);
		String fn = threadFiles.remove(op);
		List<Range> ranges = fileRanges.get(fn);
		ranges.remove(range);
	}

	public static void updateProgress(TableScanOperator.ReaderThread op, int low)
	{
		threadRanges.get(op).low = low;
	}

	public static void write(Page p, int off, byte[] data)
	{
		int hash = (p.block().hashCode2() & 0x7FFFFFFF) % mLength;
		managers[hash].write(p, off, data);
	}

	@Override
	public void run()
	{
		/*
		 * int numThreads = mLength >> 4; int x = 1; while (x < numThreads) {
		 * new OddThread(numThreads, x).start(); x++; }
		 * 
		 * int pages = managers[0].bp.length;
		 * 
		 * while (true) { int i = 0; boolean didSomething = false; while (i <
		 * pages) { int j = 0; while (j < mLength) { try { if
		 * (managers[j].cleanPage(i)) { didSomething = true; } } catch
		 * (Exception e) { HRDBMSWorker.logger.debug("", e); } j += numThreads;
		 * }
		 * 
		 * i++; }
		 * 
		 * if (!didSomething) { try { //for (String file : delayed) //{ //
		 * FileManager.endDelay(file); //}
		 * 
		 * Thread.sleep(20000); } catch (Exception e) {
		 * HRDBMSWorker.logger.debug("", e); } } }
		 */
	}

	public static class FlushThread extends HRDBMSThread
	{
		private final int i;
		private final FileChannel fc;
		private Exception e = null;
		private HashSet<String> toForce = new HashSet<String>();

		public FlushThread(int i, FileChannel fc)
		{
			this.i = i;
			this.fc = fc;
		}

		public Exception getException()
		{
			return e;
		}

		public HashSet<String> getToForce()
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
			catch (Exception e)
			{
				this.e = e;
			}
		}
	}

	public static class Range
	{
		public volatile int low;
		public final int high;

		public Range(int low, int high)
		{
			this.low = low;
			this.high = high;
		}

		@Override
		public boolean equals(Object r)
		{
			Range rhs = (Range)r;
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
		private ArrayList<Integer> cols;
		private int layoutSize;
		private Block firstBlock;
		private int num = -1;
		private int inRank = -1;
		private ArrayList<RequestPagesThread> threads;
		
		public RequestPagesThread(ArrayList<RequestPagesThread> threads)
		{
			this.threads = threads;
		}

		public RequestPagesThread(Block[] reqBlocks, long txnum)
		{
			this.reqBlocks = reqBlocks;
			this.txnum = txnum;
		}

		public RequestPagesThread(Block[] reqBlocks, long txnum, ArrayList<Integer> cols, int layoutSize)
		{
			this.reqBlocks = reqBlocks;
			this.txnum = txnum;
			this.cols = cols;
			this.layoutSize = layoutSize;
		}
		
		public RequestPagesThread(Block firstBlock, long txnum, int num, ArrayList<Integer> cols, int layoutSize, int inRank)
		{
			this.txnum = txnum;
			this.cols = cols;
			this.layoutSize = layoutSize;
			this.firstBlock = firstBlock;
			this.num = num;
			this.inRank = inRank;
		}

		@Override
		public void run()
		{
			try
			{
				if (threads != null)
				{
					for (RequestPagesThread thread : threads)
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
						int rankSize = reqBlocks.length;
						int rank = 1;
						for (final Block b : reqBlocks)
						{
							BufferManager.pin(b, txnum, rank, rankSize);
							rank++;
						}

						return;
					}
					catch (Exception e)
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
					
					ReadThread thread = BufferManager.pinConsecutive(firstBlock, txnum, num, cols, layoutSize, inRank+1, rankSize);
					thread.join();
					return;
				}
				
				ArrayList<ReadThread> threads = new ArrayList<ReadThread>();
				int rankSize = reqBlocks.length;
				try
				{
					int rank = 1;
					for (final Block b : reqBlocks)
					{
						ReadThread thread = BufferManager.pin(b, txnum, cols, layoutSize, rank, rankSize);

						if (thread != null)
						{
							threads.add(thread);
						}
						
						rank++;
					}

					for (ReadThread thread : threads)
					{
						thread.join();
					}

					return;
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
				}
				/*
				 * ArrayList<Read3Thread> threads2 = new
				 * ArrayList<Read3Thread>(); IdentityHashMap<Read3Thread,
				 * ArrayList<Block>> notNeededMap = new
				 * IdentityHashMap<Read3Thread, ArrayList<Block>>();
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
				 * (ArrayList<Block> notNeeded : notNeededMap.values()) { for
				 * (Block bl : notNeeded) { tx.unpin(tx.getPage(bl)); } }
				 */
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}

	private static class Request3PagesThread extends HRDBMSThread
	{
		private final Block[] reqBlocks;
		private final long txnum;

		public Request3PagesThread(Block[] reqBlocks, long txnum)
		{
			this.reqBlocks = reqBlocks;
			this.txnum = txnum;
		}

		@Override
		public void run()
		{
			try
			{
				// HRDBMSWorker.logger.debug("Short BM request pages thread starting");
				int rankSize = reqBlocks.length;
				int rank = 1;
				for (final Block b : reqBlocks)
				{
					BufferManager.pin3(b, txnum, rank, rankSize);
					rank++;
				}
				// HRDBMSWorker.logger.debug("Short BM request pages thread ending");
			}
			catch (Exception e)
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
		private final ArrayList<Integer> fetchPos;

		public Request3PagesThread2(Block[] reqBlocks, Transaction tx, Schema[] schemas, int schemaIndex, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos)
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
				// HRDBMSWorker.logger.debug("Long BM request pages thread starting");
				int rankSize = reqBlocks.length;
				int rank = 1;
				for (final Block b : reqBlocks)
				{
					BufferManager.pin3(b, tx, schemas[schemaIndex], schemas[schemaIndex + 1], schemas[schemaIndex + 2], schemaMap, fetchPos, rank, rankSize);
					schemaIndex += 3;
					rank++;
				}
				// HRDBMSWorker.logger.debug("Long BM request pages thread ending");
			}
			catch (Exception e)
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
		private final ArrayList<Integer> fetchPos;

		public RequestPagesThread2(Block[] reqBlocks, Transaction tx, Schema[] schemas, int schemaIndex, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos)
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
				int rankSize = reqBlocks.length;
				int rank = 1;
				for (final Block b : reqBlocks)
				{
					BufferManager.pin(b, tx, schemas[schemaIndex++], schemaMap, fetchPos, rank, rankSize);
					rank++;
				}
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}
}
