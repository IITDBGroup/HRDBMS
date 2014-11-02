package com.exascale.managers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import com.exascale.exceptions.BufferPoolExhaustedException;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.logging.DeleteLogRec;
import com.exascale.logging.InsertLogRec;
import com.exascale.logging.LogIterator;
import com.exascale.logging.LogRec;
import com.exascale.misc.MultiHashMap;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.IOThread;

public class SubBufferManager
{
	private Page[] bp;
	private int numAvailable;
	private int numNotTouched;
	private HashMap<Block, Integer> pageLookup;
	private TreeMap<Long, Block> referencedLookup;
	private TreeMap<Long, Block> unmodLookup;
	private boolean log;
	private MultiHashMap<Long, Page> myBuffers;

	public SubBufferManager(boolean log)
	{
		this.log = log;
		myBuffers = new MultiHashMap<Long, Page>();
		
		numAvailable = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("bp_pages")) / (Runtime.getRuntime().availableProcessors() * 2);
		numNotTouched = numAvailable;
		bp = new Page[numAvailable];
		int i = 0;
		while (i < numAvailable)
		{
			bp[i] = new Page();
			i++;
		}

		pageLookup = new HashMap<Block, Integer>();
		referencedLookup = new TreeMap<Long, Block>();
		unmodLookup = new TreeMap<Long, Block>();
	}
	
	public synchronized void throwAwayPage(Block b) throws Exception
	{
		for (Page p : bp)
		{
			if (b.equals(p.block()))
			{
				p.setNotModified();
				p.buffer().position(0);
				int i = 0;
				while (i < Page.BLOCK_SIZE)
				{
					p.buffer().putLong(-1);
					i += 8;
				}
			}
		}
	}

	public synchronized HashSet<String> flushAll(FileChannel fc) throws Exception
	{
		/*for (final Page p : BufferManager.bp)
		{
			if (p.isModified() && (!p.isPinned()))
			{
				p.pin(p.pinTime(), -3);
				FileManager.write(p.block(), p.buffer());

				p.unpin(-3);
			}
		}*/
		
		//for all blocks in bufferpool, if not pinned - flush, otherwise copy and rollback and flush
		HashMap<Block, Page> toRoll = new HashMap<Block, Page>();
		ArrayList<Page> toPut = new ArrayList<Page>();
		for (Page p : bp)
		{
			if (p.isModified())
			{
				if (p.isPinned())
				{
					Page clone = p.clone();
					toRoll.put(clone.block(), clone);
				}
				else
				{
					toPut.add(p);
				}
			}
		}
		
		//start put threads
		PutThread putThread = null;
		RollThread rollThread = null;
		putThread = new PutThread(toPut);
		putThread.start();
		
		//do rollbacks
		Iterator<LogRec> iter = new LogIterator(LogManager.filename, false, fc);
		while (iter.hasNext())
		{
			LogRec rec = iter.next();
			if (rec.type() == LogRec.INSERT)
			{
				InsertLogRec ins = (InsertLogRec)rec.rebuild();
				Block b = ins.getBlock();
				Page p = toRoll.get(b);
				p.writeDirect(ins.getOffset(), ins.getBefore());
			}
			else if (rec.type() == LogRec.DELETE)
			{
				DeleteLogRec del = (DeleteLogRec)rec.rebuild();
				Block b = del.getBlock();
				Page p = toRoll.get(b);
				p.writeDirect(del.getOffset(), del.getBefore());
			}
		}
		
		((LogIterator)iter).close();
		
		//for (Page p : toRoll.values())
		//{
		//	FileManager.write(p.block(), p.buffer());
		//}
		
		rollThread = new RollThread(toRoll);
		rollThread.start();
		
		HashSet<String> retval = new HashSet<String>();
		for (Page p : toPut)
		{
			retval.add(p.block().fileName());
		}
		
		for (Block b : toRoll.keySet())
		{
			retval.add(b.fileName());
		}
		
		Exception e = null;
		putThread.join();
		if (e == null)
		{
			 e = putThread.getException();
		}
		
		rollThread.join();
		if (e == null)
		{
			e = rollThread.getException();
		}
		
		if (e != null)
		{
			throw e;
		}
		
		return retval;
	}
	
	private class RollThread extends HRDBMSThread
	{
		private HashMap<Block, Page> toRoll;
		private Exception e = null;
		
		public RollThread(HashMap<Block, Page> toRoll)
		{
			this.toRoll = toRoll;
		}
		
		public Exception getException()
		{
			return e;
		}
		
		public void run()
		{
			for (Page p : toRoll.values())
			{
				try
				{
					FileManager.writeDelayed(p.block(), p.buffer());
				}
				catch(Exception e)
				{
					this.e = e;
					return;
				}
			}
		}
	}
	
	private class PutThread extends HRDBMSThread
	{
		private ArrayList<Page> toPut;
		private Exception e = null;
		
		public PutThread(ArrayList<Page> toPut)
		{
			this.toPut = toPut;
		}
		
		public Exception getException()
		{
			return e;
		}
		
		public void run()
		{
			for (Page p : toPut)
			{
				try
				{
					FileManager.writeDelayed(p.block(), p.buffer());
					p.setNotModified();
					synchronized(unmodLookup)
					{
						unmodLookup.put(LogManager.getLSN(), p.block());
					}
				}
				catch(Exception e)
				{
					this.e = e;
					return;
				}
			}
		}
	}

	public synchronized Page getPage(Block b)
	{
		final Integer index = pageLookup.get(b);

		if (index == null)
		{
			return null;
		}

		Page retval = bp[index];
		Block b2 = retval.block();
		if (!b.equals(b2))
		{
			HRDBMSWorker.logger.fatal("The block " + b + " was requested, but the BufferManager is returning block " + b2);
			System.exit(1);
			return null;
		}
		
		return retval;
	}

	public synchronized void pin(Block b, long txnum) throws Exception
	{
		int index = findExistingPage(b);
		if (index == -1)
		{
			index = chooseUnpinnedPage();

			if (numNotTouched > 0)
			{
				numNotTouched--;
			}

			if (index == -1)
			{
				HRDBMSWorker.logger.error("Buffer pool exhausted.");
				throw new BufferPoolExhaustedException();
			}

			if (bp[index].block() != null)
			{
				referencedLookup.remove(bp[index].pinTime());
				synchronized(unmodLookup)
				{
					unmodLookup.remove(bp[index].pinTime());
				}
				bp[index].setPinTime(-1);
				pageLookup.remove(bp[index].block());
			}
			bp[index].assignToBlock(b, log);
			if (pageLookup.containsKey(b))
			{
				Exception e = new Exception("About to put a duplicate page in the bufferpool");
				HRDBMSWorker.logger.debug("", e);
				throw e;
			}
			pageLookup.put(b, index);
		}

		if (bp[index].pinTime() != -1)
		{
			referencedLookup.remove(bp[index].pinTime());
			synchronized(unmodLookup)
			{
				unmodLookup.remove(bp[index].pinTime());
			}
		}

		final long lsn = LogManager.getLSN();
		referencedLookup.put(lsn, b);
		if (!bp[index].isModified())
		{
			synchronized(unmodLookup)
			{
				unmodLookup.put(lsn, b);
			}
		}
		bp[index].pin(lsn, txnum);
		myBuffers.multiPut(txnum, bp[index]);
	}
	
	public synchronized void pinFromMemory(Block b, long txnum, ByteBuffer data) throws Exception
	{
		int index;
		
		index = chooseUnpinnedPage();

		if (numNotTouched > 0)
		{
			numNotTouched--;
		}

		if (index == -1)
		{
			HRDBMSWorker.logger.error("Buffer pool exhausted.");
			throw new BufferPoolExhaustedException();
		}

		if (bp[index].block() != null)
		{
			referencedLookup.remove(bp[index].pinTime());
			synchronized(unmodLookup)
			{
				unmodLookup.remove(bp[index].pinTime());
			}
			bp[index].setPinTime(-1);
			pageLookup.remove(bp[index].block());
		}
		bp[index].assignToBlockFromMemory(b, log, data);
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
			synchronized(unmodLookup)
			{
				unmodLookup.remove(bp[index].pinTime());
			}
		}

		final long lsn = LogManager.getLSN();
		referencedLookup.put(lsn, b);
		if (!bp[index].isModified())
		{
			synchronized(unmodLookup)
			{
				unmodLookup.put(lsn, b);
			}
		}
		bp[index].pin(lsn, txnum);
		myBuffers.multiPut(txnum, bp[index]);
	}

	public synchronized void unpin(Page p, long txnum)
	{
		p.unpin(txnum);
		myBuffers.multiRemove(txnum, p);
	}

	public synchronized void unpinAll(long txnum)
	{
		for (final Page p : myBuffers.get(txnum))
		{
			p.unpin(txnum);
		}
	}

	public void write(Page p, int off, byte[] data)
	{
		p.buffer().position(off);
		p.buffer().put(data);
		synchronized(unmodLookup)
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

		synchronized(unmodLookup)
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

	public void requestPage(Block b, long txnum)
	{
		try
		{
			pin(b, txnum);
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.warn("Error fetching pages", e);
		}
	}
}
