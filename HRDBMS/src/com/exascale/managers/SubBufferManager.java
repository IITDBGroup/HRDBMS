package com.exascale.managers;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import com.exascale.exceptions.BufferPoolExhaustedException;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.logging.DeleteLogRec;
import com.exascale.logging.InsertLogRec;
import com.exascale.logging.LogIterator;
import com.exascale.logging.LogRec;
import com.exascale.misc.MultiHashMap;
import com.exascale.tables.Schema;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;

public class SubBufferManager
{
	Page[] bp;
	private final int numAvailable;
	private int numNotTouched;
	private final HashMap<Block, Integer> pageLookup;
	private final boolean log;
	private final MultiHashMap<Long, Page> myBuffers;
	private volatile int clock = 0;

	public SubBufferManager(boolean log)
	{
		this.log = log;
		myBuffers = new MultiHashMap<Long, Page>();

		numAvailable = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("bp_pages")) / (Runtime.getRuntime().availableProcessors() << 5);
		numNotTouched = numAvailable;
		bp = new Page[numAvailable];
		clock = numAvailable / 2;
		int i = 0;
		while (i < numAvailable)
		{
			bp[i] = new Page();
			i++;
		}

		pageLookup = new HashMap<Block, Integer>(numAvailable);
	}

	public synchronized boolean cleanPage(int i) throws Exception
	{
		if (i >= bp.length)
		{
			return false;
		}
		
		Page p = bp[i];
		if (p.isModified() && !p.isPinned())
		{
			p.setNotModified();
			FileManager.writeDelayed(p.block(), p.buffer());
			return true;
		}

		return false;
	}

	public synchronized HashSet<String> flushAll(FileChannel fc) throws Exception
	{
		ArrayList<Page> toPut = new ArrayList<Page>();
		for (Page p : bp)
		{
			if (p.isModified())
			{
				toPut.add(p);
			}
		}

		// start put threads
		PutThread putThread = null;
		putThread = new PutThread(toPut);
		putThread.start();

		HashSet<String> retval = new HashSet<String>();
		for (Page p : toPut)
		{
			retval.add(p.block().fileName());
		}

		Exception e = null;
		putThread.join();
		if (e == null)
		{
			e = putThread.getException();
		}

		if (e != null)
		{
			throw e;
		}

		return retval;
	}

	public synchronized Page getPage(Block b, long txnum)
	{
		final Integer index = pageLookup.get(b);

		if (index == null)
		{
			return null;
		}

		Page retval = bp[index];
		
		if (!retval.isPinnedBy(txnum))
		{
			return null;
		}
		
		Block b2 = retval.block();
		if (!b.equals(b2))
		{
			HRDBMSWorker.logger.fatal("The block " + b + " was requested, but the BufferManager is returning block " + b2);
			System.exit(1);
			return null;
		}

		return retval;
	}

	public synchronized void invalidateFile(String fn) throws Exception
	{
		for (Page p : bp)
		{
			if (p.block() != null)
			{
				if (p.block().fileName().contains("/" + fn))
				{
					p.setPinTime(-1);
					pageLookup.remove(p.block());
					p.assignToBlock(null, log);
				}
			}
		}
	}

	public void pin(Block b, long txnum) throws Exception
	{
		// Transaction.txListLock.readLock().lock();
		{
			try
			{
				boolean wait = false;
				while (true)
				{
					if (wait)
					{
						LockSupport.parkNanos(500);
					}

					synchronized (this)
					{
						int index = findExistingPage(b);
						if (index == -1)
						{
							if (!Transaction.txListLock.writeLock().tryLock())
							{
								wait = true;
								continue;
							}

							index = chooseUnpinnedPage(b.fileName());

							if (index == -1)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched--;
							}

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}
							try
							{
								bp[index].assignToBlock(b, log);
							}
							catch (Exception e)
							{
								Transaction.txListLock.writeLock().unlock();
								throw e;
							}

							pageLookup.put(b, index);
							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							Transaction.txListLock.writeLock().unlock();
							return;
						}
						else
						{
							if (!Transaction.txListLock.readLock().tryLock())
							{
								wait = true;
								continue;
							}
							
							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							Transaction.txListLock.readLock().unlock();
							return;
						}

					}
				}
			}
			catch (Exception e)
			{
				Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
	}
	
	public void pin(Block b, Transaction tx, Schema schema, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos) throws Exception
	{
		// Transaction.txListLock.readLock().lock();
		long txnum = tx.number();
		{
			try
			{
				boolean wait = false;
				while (true)
				{
					if (wait)
					{
						LockSupport.parkNanos(500);
					}

					synchronized (this)
					{
						int index = findExistingPage(b);
						if (index == -1)
						{
							if (!Transaction.txListLock.writeLock().tryLock())
							{
								wait = true;
								continue;
							}

							index = chooseUnpinnedPage(b.fileName());

							if (index == -1)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched--;
							}

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}
							try
							{
								bp[index].assignToBlock(b, log, schema, schemaMap, tx, fetchPos);
							}
							catch (Exception e)
							{
								Transaction.txListLock.writeLock().unlock();
								throw e;
							}
							pageLookup.put(b, index);
							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							Transaction.txListLock.writeLock().unlock();
							return;
						}
						else
						{
							if (!Transaction.txListLock.readLock().tryLock())
							{
								wait = true;
								continue;
							}
							
							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							new ParseThread(bp[index], schema, tx, schemaMap, fetchPos).start();
							Transaction.txListLock.readLock().unlock();
							return;
						}
					}
				}
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
	}
	
	private class ParseThread extends HRDBMSThread
	{
		private Page p;
		private Schema schema;
		private Transaction tx;
		private ConcurrentHashMap<Integer, Schema> schemaMap;
		private ArrayList<Integer> fetchPos;
		
		public ParseThread(Page p, Schema schema, Transaction tx, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos)
		{
			this.p = p;
			this.schema = schema;
			this.tx = tx;
			this.schemaMap = schemaMap;
			this.fetchPos = fetchPos;
		}
		
		public void run()
		{
			try
			{
				tx.read2(p.block(), schema, p);
				schemaMap.put(p.block().number(), schema);
				schema.prepRowIter(fetchPos);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}

	public void pinFromMemory(Block b, long txnum, ByteBuffer data) throws Exception
	{
		// Transaction.txListLock.readLock().lock();
		{
			try
			{
				boolean wait = false;
				while (true)
				{
					if (wait)
					{
						LockSupport.parkNanos(500);
					}

					synchronized (this)
					{
						int index;

						if (!Transaction.txListLock.writeLock().tryLock())
						{
							wait = true;
							continue;
						}

						index = chooseUnpinnedPage(b.fileName());

						if (index == -1)
						{
							HRDBMSWorker.logger.error("Buffer pool exhausted.");
							throw new BufferPoolExhaustedException();
						}

						if (numNotTouched > 0)
						{
							numNotTouched--;
						}

						if (bp[index].block() != null)
						{
							bp[index].setPinTime(-1);
							pageLookup.remove(bp[index].block());
						}
						try
						{
							bp[index].assignToBlockFromMemory(b, log, data);
						}
						catch (Exception e)
						{
							Transaction.txListLock.writeLock().unlock();
							throw e;
						}
						
						pageLookup.put(b, index);
						final long lsn = LogManager.getLSN();
						bp[index].pin(lsn, txnum);
						myBuffers.multiPut(txnum, bp[index]);
						Transaction.txListLock.writeLock().unlock();
						return;
					}
				}
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
	}

	public void requestPage(Block b, long txnum)
	{
		try
		{
			pin(b, txnum);
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.warn("Error fetching pages", e);
		}
	}

	public synchronized void throwAwayPage(Block b) throws Exception
	{
		Page p = bp[pageLookup.get(b)];
		p.setNotModified();
		p.setPinTime(-1);
		pageLookup.remove(p.block());
	}

	public synchronized void unpin(Page p, long txnum)
	{
		if (p.unpin1(txnum))
		{
			myBuffers.multiRemove(txnum, p);
		}
	}

	public synchronized void unpinAll(long txnum)
	{
		for (final Page p : myBuffers.get(txnum))
		{
			p.unpin(txnum);
		}

		myBuffers.remove(txnum);
	}

	public synchronized void unpinAllExcept(long txnum, String iFn, Block inUse)
	{
		for (final Page p : myBuffers.get(txnum))
		{
			if (p.block().fileName().equals(iFn) && p.block().number() != inUse.number())
			{
				p.unpin(txnum);
				myBuffers.multiRemove(txnum, p);
			}
		}
	}

	public synchronized void unpinAllLessThan(long txnum, int lt, String tFn)
	{
		String prefix = tFn.substring(0, tFn.lastIndexOf('/') + 1);
		for (final Page p : myBuffers.get(txnum))
		{
			if (p.block().fileName().startsWith(prefix) && (p.block().fileName().endsWith("indx") || p.block().number() < lt-1))
			{
				p.unpin(txnum);
				myBuffers.multiRemove(txnum, p);
			}
		}
	}

	public synchronized void unpinAllLessThan(long txnum, int lt, String tFn, boolean flag)
	{
		for (final Page p : myBuffers.get(txnum))
		{
			if (p.block().fileName().equals(tFn) && p.block().number() < lt)
			{
				p.unpin(txnum);
				myBuffers.multiRemove(txnum, p);
			}
		}
	}

	public synchronized void unpinMyDevice(long txnum, String prefix)
	{
		for (final Page p : myBuffers.get(txnum))
		{
			if (p.block().fileName().startsWith(prefix))
			{
				p.unpin(txnum);
				myBuffers.multiRemove(txnum, p);
			}
		}
	}

	public void write(Page p, int off, byte[] data)
	{
		p.buffer().position(off);
		p.buffer().put(data);
	}

	private synchronized int chooseUnpinnedPage(String newFN)
	{
		if (numNotTouched > 0)
		{
			return numAvailable - numNotTouched;
		}
		
		//String partial = newFN.substring(newFN.lastIndexOf('/') + 1);

		//int checked = 0;
		int initialClock = clock;
		boolean start = true;

		while (start || clock != initialClock)
		{
			start = false;
			Page p = bp[clock];
			int index = clock;
			clock++;
			if (clock == bp.length)
			{
				clock = 0;
			}
			if (!p.isPinned() && !BufferManager.isInterest(p.block()))
			{
				return index;
			}
		}
		
		start = true;

		while (start || clock != initialClock)
		{
			start = false;
			Page p = bp[clock];
			int index = clock;
			clock++;
			if (clock == bp.length)
			{
				clock = 0;
			}
			if (!p.isPinned())
			{
				return index;
			}
		}

		//expand bufferpool
		Page[] bp2 = new Page[(int)(bp.length * 1.1)];
		System.arraycopy(bp, 0, bp2, 0, bp.length);
		int z = bp.length;
		final int limit = bp2.length;
		while (z < limit)
		{
			bp2[z++] = new Page();
		}
		
		bp = bp2;
		return chooseUnpinnedPage(newFN);
	}

	private synchronized int findExistingPage(Block b)
	{
		final Integer temp = pageLookup.get(b);
		if (temp == null)
		{
			return -1;
		}

		return temp.intValue();
	}

	private class PutThread extends HRDBMSThread
	{
		private final ArrayList<Page> toPut;
		private Exception e = null;

		public PutThread(ArrayList<Page> toPut)
		{
			this.toPut = toPut;
		}

		public Exception getException()
		{
			return e;
		}

		@Override
		public void run()
		{
			for (Page p : toPut)
			{
				try
				{
					FileManager.writeDelayed(p.block(), p.buffer());
					if (!p.isPinned())
					{
						p.setNotModified();
					}
				}
				catch (Exception e)
				{
					this.e = e;
					return;
				}
			}
		}
	}

	private class RollThread extends HRDBMSThread
	{
		private final HashMap<Block, Page> toRoll;
		private Exception e = null;

		public RollThread(HashMap<Block, Page> toRoll)
		{
			this.toRoll = toRoll;
		}

		public Exception getException()
		{
			return e;
		}

		@Override
		public void run()
		{
			for (Page p : toRoll.values())
			{
				try
				{
					FileManager.writeDelayed(p.block(), p.buffer());
				}
				catch (Exception e)
				{
					this.e = e;
					return;
				}
			}
		}
	}
}
