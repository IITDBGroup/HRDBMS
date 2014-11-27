package com.exascale.managers;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
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
import com.exascale.managers.FileManager.EndDelayThread;
import com.exascale.misc.MultiHashMap;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.IOThread;

public class BufferManager
{
	public static SubBufferManager[] managers;

	public BufferManager(boolean log)
	{
		managers = new SubBufferManager[Runtime.getRuntime().availableProcessors() * 2];
		int i = 0;
		while (i < managers.length)
		{
			managers[i] = new SubBufferManager(log); 
			i++;
		}
	}
	
	public static void throwAwayPage(String fn, int blockNum) throws Exception
	{
		Block b = new Block(fn, blockNum);
		int hash = (b.hashCode2() & 0x7FFFFFFF) % managers.length;
		managers[hash].throwAwayPage(b);
	}
	
	public static void invalidateFile(String fn) throws Exception
	{
		for (SubBufferManager manager : managers)
		{
			manager.invalidateFile(fn);
		}
	}

	public static void flushAll(FileChannel fc) throws Exception
	{
		ArrayList<FlushThread> threads = new ArrayList<FlushThread>();
		int i = 0;
		while (i < managers.length)
		{
			threads.add(new FlushThread(managers[i], fc));
			i++;
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
	
	public static class FlushThread extends HRDBMSThread
	{
		private SubBufferManager manager;
		private FileChannel fc;
		private Exception e = null;
		private HashSet<String> toForce = new HashSet<String>();
		
		public FlushThread(SubBufferManager manager, FileChannel fc)
		{
			this.manager = manager;
			this.fc = fc;
		}
		
		public void run()
		{
			try
			{
				toForce = manager.flushAll(fc);
			}
			catch(Exception e)
			{
				this.e = e;
			}
		}
		
		public HashSet<String> getToForce()
		{
			return toForce;
		}
		
		public Exception getException()
		{
			return e;
		}
	}

	public static Page getPage(Block b)
	{
		int hash = (b.hashCode2() & 0x7FFFFFFF) % managers.length;
		return managers[hash].getPage(b);
	}

	public static void pin(Block b, long txnum) throws Exception
	{
		int hash = (b.hashCode2() & 0x7FFFFFFF) % managers.length;
		managers[hash].pin(b, txnum);
	}

	public static void unpin(Page p, long txnum)
	{
		int hash = (p.block().hashCode2() & 0x7FFFFFFF) % managers.length;
		managers[hash].unpin(p, txnum);
	}

	public static void unpinAll(long txnum)
	{
		int i = 0;
		while (i < managers.length)
		{
			managers[i].unpinAll(txnum);
			i++;
		}
	}
	
	public static void unpinAll2(long txnum)
	{
		int i = 0;
		while (i < managers.length)
		{
			managers[i].unpinAll2(txnum);
			i++;
		}
	}

	public static void write(Page p, int off, byte[] data)
	{
		int hash = (p.block().hashCode2() & 0x7FFFFFFF) % managers.length;
		managers[hash].write(p, off, data);
	}

	public static void requestPage(Block b, long txnum)
	{
		try
		{
			int hash = (b.hashCode2() & 0x7FFFFFFF) % managers.length;
			managers[hash].requestPage(b, txnum);
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.warn("Error fetching pages", e);
		}
	}

	public static void requestPages(Block[] reqBlocks, long txnum)
	{
		try
		{
			new IOThread(reqBlocks, txnum).start();
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.warn("Error fetching pages", e);
		}
	}
}
