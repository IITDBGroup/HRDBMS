package com.exascale.managers;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.exascale.exceptions.BufferPoolExhaustedException;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.filesystem.PageCleaner;
import com.exascale.misc.MultiHashMap;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.IOThread;

public class BufferManager extends HRDBMSThread
{
	private static BlockingQueue<String> in = new LinkedBlockingQueue<String>();
	public static Page[] bp;
	private static int numAvailable;
	public static int numNotTouched;
	private static HashMap<Block, Integer> pageLookup;
	private static TreeMap<Long, Block> referencedLookup;
	public static TreeMap<Long, Block> unmodLookup;
	private static boolean log;
	private static MultiHashMap<Long, Page> myBuffers;
	
	public BufferManager(boolean log)
	{
		HRDBMSWorker.logger.info("Starting initialization of the Buffer Manager.");
		this.log = log;
		myBuffers = new MultiHashMap<Long, Page>();
		numAvailable = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("bp_pages"));
		numNotTouched = numAvailable;
		this.setWait(true);
		this.description = "Buffer Manager";
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
	
	public void run()
	{
		HRDBMSWorker.addThread(new PageCleaner());
		HRDBMSWorker.logger.info("Buffer Manager initialization complete.");
		try
		{
			while (true)
			{
				processCommand(in.take());
			}
		}
		catch(InterruptedException e)
		{}
	}
	
	private void processCommand(String cmd)
	{
		if (cmd.startsWith("REQUEST PAGES"))
		{
			requestPages(cmd);
		}
		else if (cmd.startsWith("REQUEST PAGE"))
		{
			requestPage(cmd);
		}
		else
		{
			HRDBMSWorker.logger.error("Unknown message received by the Buffer Manager: " + cmd);
			in = null;
			this.terminate();
			return;
		}
	}
	
	private void requestPage(String cmd)
	{
		cmd = cmd.substring((13));
		StringTokenizer tokens = new StringTokenizer(cmd,"~", false);
		long txnum = Long.parseLong(tokens.nextToken());
		String filename = tokens.nextToken();
		int number = Integer.parseInt(tokens.nextToken());
		HRDBMSWorker.addThread(new IOThread(new Block(filename, number), txnum));
	}
	
	private void requestPages(String cmd)
	{
		cmd = cmd.substring((14));
		StringTokenizer tokens = new StringTokenizer(cmd,"~", false);
		long txnum = Long.parseLong(tokens.nextToken());
		int numBlocks = Integer.parseInt(tokens.nextToken());
		int i = 0;
		Block[] reqBlocks = new Block[numBlocks];
		while (i < numBlocks)
		{
			String filename = tokens.nextToken();
			int number = Integer.parseInt(tokens.nextToken());
			reqBlocks[i] = new Block(filename, number);
			i++;
		}
		HRDBMSWorker.addThread(new IOThread(reqBlocks, txnum));
	}
	
	public static synchronized void pin(Block b, long txnum) throws BufferPoolExhaustedException, IOException
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
				unmodLookup.remove(bp[index].pinTime());
				bp[index].setPinTime(-1);
				pageLookup.remove(bp[index].block());
			}
			bp[index].assignToBlock(b, log);
			pageLookup.put(b, index);
		}
		
		if (!bp[index].isPinned())
		{
			numAvailable--;
		}
		
		if (bp[index].pinTime() != -1)
		{
			referencedLookup.remove(bp[index].pinTime());
			unmodLookup.remove(bp[index].pinTime());
		}
		
		long lsn = LogManager.getLSN();
		referencedLookup.put(lsn, b);
		if (!bp[index].isModified())
		{
			unmodLookup.put(lsn, b);
		}
		bp[index].pin(lsn, txnum);
		myBuffers.multiPut(txnum, bp[index]);
	}
	
	public static synchronized void unpin(Page p, long txnum)
	{
		p.unpin(txnum);
		myBuffers.multiRemove(txnum, p);
		if (!p.isPinned())
		{
			numAvailable++;
		}
	}
	
	public static synchronized void unpinAll(long txnum)
	{
		for (Page p : myBuffers.get(txnum))
		{
			p.unpin(txnum);
		}
	}
	
	public static synchronized int available()
	{
		return numAvailable;
	}
	
	private static synchronized int findExistingPage(Block b)
	{
		Integer temp = pageLookup.get(b);
		if (temp == null)
		{
			return -1;
		}
		
		return temp.intValue();
	}
	
	private static synchronized int chooseUnpinnedPage()
	{
		if (numNotTouched > 0)
		{
			return numAvailable - numNotTouched;
		}
		
		if (!unmodLookup.isEmpty())
		{
			for (Block b : unmodLookup.values())
			{
				int index = pageLookup.get(b);
				if (!bp[index].isPinned())
				{
					return index;
				}
			}
		}
		
		for (Block b : referencedLookup.values())
		{
			int index = pageLookup.get(b);
			if (!bp[index].isPinned())
			{
				return index;
			}
		}
		
		return -1;
	}
	
	public static BlockingQueue<String> getInputQueue()
	{
		return in;
	}
	
	public static synchronized Page getPage(Block b)
	{
		Integer index = pageLookup.get(b);
		
		if (index == null)
		{
			return null;
		}
		
		return bp[index];
	}
	
	public static synchronized void write(Page p, int off, byte[] data)
	{
		p.buffer().position(off);
		p.buffer().put(data);
		unmodLookup.remove(p.pinTime());
	}
	
	public static void flushAll() throws IOException
	{
		for (Page p : BufferManager.bp)
		{
			if (p.isModified() && (!p.isPinned()))
			{
				p.pin(p.pinTime(), -3);
				FileManager.write(p.block(), p.buffer());
			
				p.unpin(-3);
			}
		}
	}
}
