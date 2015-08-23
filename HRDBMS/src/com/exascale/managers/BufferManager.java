package com.exascale.managers;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.managers.FileManager.EndDelayThread;
import com.exascale.misc.HJOMultiHashMap;
import com.exascale.misc.MultiHashMap;
import com.exascale.misc.VHJOMultiHashMap;
import com.exascale.optimizer.TableScanOperator;
import com.exascale.tables.Schema;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.IOThread;

public class BufferManager extends HRDBMSThread
{
	public static SubBufferManager[] managers;
	private final static int mLength;
	private final static ConcurrentHashMap<TableScanOperator.ReaderThread, Range> threadRanges = new ConcurrentHashMap<TableScanOperator.ReaderThread, Range>();
	private final static VHJOMultiHashMap<String, Range> fileRanges = new VHJOMultiHashMap<String, Range>();
	private final static ConcurrentHashMap<TableScanOperator.ReaderThread, String> threadFiles = new ConcurrentHashMap<TableScanOperator.ReaderThread, String>();
	
	public static class Range
	{
		public volatile int low;
		public final int high;
		
		public Range(int low, int high)
		{
			this.low = low;
			this.high = high;
		}
		
		public boolean equals(Object r)
		{
			Range rhs = (Range)r;
			return low == rhs.low && high == rhs.high;
		}
		
		public int hashCode()
		{
			int hash = 23;
			hash = hash * 31 + low;
			hash = hash * 31 + high;
			return hash;
		}
	}

	static
	{
		mLength = Runtime.getRuntime().availableProcessors() << 5;
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
	
	public static void registerInterest(TableScanOperator.ReaderThread op, String fn, int low, int high)
	{
		Range range = new Range(low, high);
		threadRanges.put(op, range);
		fileRanges.multiPut(fn, range);
		threadFiles.put(op, fn);
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
			catch(Exception e)
			{}
		}
		
		return false;
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
		int hash = (b.hashCode2() & 0x7FFFFFFF) & (mLength - 1);
		return managers[hash].getPage(b, txnum);
	}

	public static void invalidateFile(String fn) throws Exception
	{
		for (SubBufferManager manager : managers)
		{
			manager.invalidateFile(fn);
		}
	}

	public static void pin(Block b, long txnum) throws Exception
	{
		int hash = (b.hashCode2() & 0x7FFFFFFF) & (mLength - 1);
		managers[hash].pin(b, txnum);
	}
	
	public static void pin(Block b, Transaction tx, Schema schema, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos) throws Exception
	{
		int hash = (b.hashCode2() & 0x7FFFFFFF) & (mLength - 1);
		managers[hash].pin(b, tx, schema, schemaMap, fetchPos);
	}

	public static void requestPage(Block b, long txnum)
	{
		try
		{
			int hash = (b.hashCode2() & 0x7FFFFFFF) & (mLength - 1);
			managers[hash].requestPage(b, txnum);
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.warn("Error fetching pages", e);
		}
	}

	public static void requestPages(Block[] reqBlocks, long txnum) throws Exception
	{
		for (final Block b : reqBlocks)
		{
			BufferManager.pin(b, txnum);
		}
	}
	
	public static void requestPages(Block[] reqBlocks, Transaction tx, Schema[] schemas, int schemaIndex, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos) throws Exception
	{
		for (final Block b : reqBlocks)
		{
			BufferManager.pin(b, tx, schemas[schemaIndex++], schemaMap, fetchPos);
		}
	}

	public static void throwAwayPage(String fn, int blockNum) throws Exception
	{
		Block b = new Block(fn, blockNum);
		int hash = (b.hashCode2() & 0x7FFFFFFF) & (mLength - 1);
		managers[hash].throwAwayPage(b);
	}

	public static void unpin(Page p, long txnum)
	{
		int hash = (p.block().hashCode2() & 0x7FFFFFFF) & (mLength - 1);
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

	public static void write(Page p, int off, byte[] data)
	{
		int hash = (p.block().hashCode2() & 0x7FFFFFFF) & (mLength - 1);
		managers[hash].write(p, off, data);
	}

	@Override
	public void run()
	{
		int numThreads = mLength >> 4;
		int x = 1;
		while (x < numThreads)
		{
			new OddThread(numThreads, x).start();
			x++;
		}

		int pages = managers[0].bp.length;

		while (true)
		{
			int i = 0;
			boolean didSomething = false;
			while (i < pages)
			{
				int j = 0;
				while (j < mLength)
				{
					try
					{
						if (managers[j].cleanPage(i))
						{
							didSomething = true;
						}
					}
					catch (Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
					}
					j += numThreads;
				}

				i++;
			}

			if (!didSomething)
			{
				try
				{
					//for (String file : delayed)
					//{
					//	FileManager.endDelay(file);
					//}

					Thread.sleep(20000);
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
				}
			}
		}
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
				while (j < i + 32)
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

	private static class OddThread extends HRDBMSThread
	{
		private final int add;
		private final int start;

		public OddThread(int add, int start)
		{
			this.add = add;
			this.start = start;
		}

		@Override
		public void run()
		{
			int pages = managers[0].bp.length;

			while (true)
			{
				int i = 0;
				boolean didSomething = false;
				while (i < pages)
				{
					int j = start;
					while (j < mLength)
					{
						try
						{
							if (managers[j].cleanPage(i))
							{
								didSomething = true;
							}
						}
						catch (Exception e)
						{
							HRDBMSWorker.logger.debug("", e);
						}
						j += add;
					}

					i++;
				}

				if (!didSomething)
				{
					try
					{
						//for (String file : d)
						//{
						//	FileManager.endDelay(file);
						//}

						Thread.sleep(20000);
					}
					catch (Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
					}
				}
			}
		}
	}
}
