package com.exascale;

import java.io.IOException;
import java.util.HashSet;

import com.exascale.Schema.FieldValue;

public class Transaction 
{
	public static final int ISOLATION_RR = 0, ISOLATION_CS = 1, ISOLATION_UR = 2;
	private static long nextTxNum = 0;
	public static HashSet<Long> txList = new HashSet<Long>();
	private long txnum;
	private int level;
	
	public Transaction(int level)
	{
		this.level = level;
		while (true)
		{
			synchronized(txList)
			{
				txnum = nextTx();
				txList.add(txnum);
				LogRec rec = new StartLogRec(txnum);
				LogManager.write(rec);
				break;
			}
		}
	}
	
	public void commit() throws IOException
	{
		synchronized(txList)
		{
			BufferManager.unpinAll(txnum);
			LogManager.commit(txnum);
			LockManager.release(txnum);
			txList.remove(txnum);
		}
	}
	
	public DeleteLogRec delete(byte[] before, byte[] after, int off, Block b)
	{
		return LogManager.delete(txnum, b, off, before, after);
	}
	
	public InsertLogRec insert(byte[] before, byte[] after, int off, Block b)
	{
		return LogManager.insert(txnum, b, off, before, after);
	}
	
	public void rollback() throws IOException
	{
		synchronized(txList)
		{
			BufferManager.unpinAll(txnum);
			LogManager.rollback(txnum);
			LockManager.release(txnum);
			txList.remove(txnum);
		}
	}
	
	public void requestPage(Block b)
	{
		while (true)
		{
			try
			{
				BufferManager.getInputQueue().put("REQUEST PAGE " + b.fileName() + "~" + b.number());
				break;
			}
			catch(InterruptedException e)
			{
				continue;
			}
		}
	}
	
	public void requestPages(Block[] bs)
	{
		String cmd = "REQUEST PAGES " + bs.length + "~";
		for (Block b : bs)
		{
			cmd += b.fileName() + "~" + b.number() + "~";
		}
		
		while (true)
		{	
			try
			{
				BufferManager.getInputQueue().put(cmd);
				break;
			}
			catch(InterruptedException e)
			{
				continue;
			}
		}
	}
	
	private Page getPage(Block b)
	{
		Page p = BufferManager.getPage(b);
		
		int requests = 0;
		while (p == null && requests < Integer.parseInt(HRDBMSWorker.getHParms().getProperty("getpage_rerequest_attempts")))
		{
			int sleeps = 0;
			while (p == null && sleeps < Integer.parseInt(HRDBMSWorker.getHParms().getProperty("getpage_attempts_before_rerequest")))
			{
				try
				{
					Thread.sleep(Long.parseLong(HRDBMSWorker.getHParms().getProperty("getpage_fail_sleep_time")));
				}
				catch(InterruptedException e)
				{}
			
				p = BufferManager.getPage(b);
				sleeps++;
			}
			
			while (true)
			{
				try
				{
					BufferManager.getInputQueue().put("REQUEST PAGE " + b.fileName() + "~" + b.number());
					break;
				}
				catch(InterruptedException e)
				{}
			}
		}
			
		if (p == null)
		{
			return null;
		}
		
		return p;
	}
		
	public void unpin(Page p)
	{
		BufferManager.unpin(p, txnum);
	}
	
	private static synchronized long nextTx()
	{
		nextTxNum++;
		return nextTxNum;
	}
	
	public void read(Block b, Schema schema) throws LockAbortException
	{
		if (level == ISOLATION_RR || level == ISOLATION_CS)
		{
			LockManager.sLock(b, txnum);
		}
		Page p = this.getPage(b);
		schema.read(this, p);
		if (level == ISOLATION_CS)
		{
			LockManager.unlockSLock(b, txnum);
		}
	}
	
	public HeaderPage readHeaderPage(Block b, int type) throws LockAbortException
	{
		if (level == ISOLATION_RR || level == ISOLATION_CS)
		{
			LockManager.sLock(b, txnum);
		}
		Page p = this.getPage(b);
		HeaderPage retval;
		int first = p.getInt(0);
		if (b.number() == 0 || first != -1)
		{
			retval = new HeaderPage(p, type);
		}
		else
		{
			retval = null;
		}
		
		if (level == ISOLATION_CS)
		{
			LockManager.unlockSLock(b, txnum);
		}
		
		return retval;
	}
	
	public int getIsolationLevel()
	{
		return level;
	}
	
	public void setIsolationLevel(int level)
	{
		this.level = level;
	}
	
	public long number()
	{
		return txnum;
	}
	
	//call LogManager.
}
	
