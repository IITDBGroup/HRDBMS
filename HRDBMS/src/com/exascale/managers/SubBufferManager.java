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
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;

public class SubBufferManager
{
	final Page[] bp;
	private final int numAvailable;
	private int numNotTouched;
	private final HashMap<Block, Integer> pageLookup;
	private final TreeMap<Long, Block> referencedLookup;
	private final ConcurrentHashMap<Long, Block> unmodLookup;
	private final boolean log;
	private final MultiHashMap<Long, Page> myBuffers;

	public SubBufferManager(boolean log)
	{
		this.log = log;
		myBuffers = new MultiHashMap<Long, Page>();

		numAvailable = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("bp_pages")) / (Runtime.getRuntime().availableProcessors() << 5);
		numNotTouched = numAvailable;
		bp = new Page[numAvailable];
		int i = 0;
		while (i < numAvailable)
		{
			bp[i] = new Page();
			i++;
		}

		pageLookup = new HashMap<Block, Integer>(numAvailable);
		referencedLookup = new TreeMap<Long, Block>();
		unmodLookup = new ConcurrentHashMap<Long, Block>(numAvailable, 1.0f, 64 * ResourceManager.cpus);
	}

	public synchronized boolean cleanPage(int i, HashSet<String> d) throws Exception
	{
		Page p = bp[i];
		if (p.isModified() && !p.isPinned())
		{
			p.setNotModified();
			FileManager.writeDelayed(p.block(), p.buffer());
			unmodLookup.put(p.pinTime(), p.block());
			d.add(p.block().fileName());
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
					referencedLookup.remove(p.pinTime());
					unmodLookup.remove(p.pinTime());
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
						LockSupport.parkNanos(100000);
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

							index = chooseUnpinnedPage();

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
								referencedLookup.remove(bp[index].pinTime());
								unmodLookup.remove(bp[index].pinTime());
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
							Transaction.txListLock.readLock().lock();
							Transaction.txListLock.writeLock().unlock();
							if (pageLookup.containsKey(b))
							{
								Exception e = new Exception("About to put a duplicate page in the bufferpool");
								HRDBMSWorker.logger.debug("", e);
								throw e;
							}
							pageLookup.put(b, index);
						}
						else
						{
							if (!Transaction.txListLock.readLock().tryLock())
							{
								wait = true;
								continue;
							}
						}

						if (bp[index].pinTime() != -1)
						{
							referencedLookup.remove(bp[index].pinTime());
							unmodLookup.remove(bp[index].pinTime());
						}

						final long lsn = LogManager.getLSN();
						referencedLookup.put(lsn, b);
						if (!bp[index].isModified())
						{
							unmodLookup.put(lsn, b);
						}
						bp[index].pin(lsn, txnum);
						myBuffers.multiPut(txnum, bp[index]);
					}

					break;
				}
			}
			catch (Exception e)
			{
				Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
		Transaction.txListLock.readLock().unlock();
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
						LockSupport.parkNanos(100000);
					}

					synchronized (this)
					{
						int index;

						if (!Transaction.txListLock.writeLock().tryLock())
						{
							wait = true;
							continue;
						}

						index = chooseUnpinnedPage();

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
							referencedLookup.remove(bp[index].pinTime());
							unmodLookup.remove(bp[index].pinTime());
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
						Transaction.txListLock.readLock().lock();
						Transaction.txListLock.writeLock().unlock();
						if (pageLookup.containsKey(b))
						{
							Exception e = new Exception("About to put a duplicate page in the bufferpool " + b);
							HRDBMSWorker.logger.debug("", e);
							throw e;
						}
						pageLookup.put(b, index);

						if (bp[index].pinTime() != -1)
						{
							referencedLookup.remove(bp[index].pinTime());
							unmodLookup.remove(bp[index].pinTime());
						}

						final long lsn = LogManager.getLSN();
						referencedLookup.put(lsn, b);
						//if (!bp[index].isModified())
						//{
						//	unmodLookup.put(lsn, b);
						//}
						bp[index].pin(lsn, txnum);
						myBuffers.multiPut(txnum, bp[index]);
					}

					break;
				}
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
		Transaction.txListLock.readLock().unlock();
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
		referencedLookup.remove(p.pinTime());
		unmodLookup.remove(p.pinTime());
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
		synchronized (unmodLookup)
		{
			unmodLookup.remove(p.pinTime());
		}
	}

	private synchronized int chooseUnpinnedPage()
	{
		if (numNotTouched > 0)
		{
			return numAvailable - numNotTouched;
		}

		synchronized (unmodLookup)
		{
			if (!unmodLookup.isEmpty())
			{
				for (final Block b : unmodLookup.values())
				{
					final int index = pageLookup.get(b);
					if (!bp[index].isPinned())
					{
						return index;
					}
				}
			}
		}

		for (final Block b : referencedLookup.values())
		{
			final int index = pageLookup.get(b);
			if (!bp[index].isPinned())
			{
				return index;
			}
		}

		return -1;
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
						unmodLookup.put(p.pinTime(), p.block());
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
