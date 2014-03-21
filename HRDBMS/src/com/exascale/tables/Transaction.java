package com.exascale.tables;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.exceptions.LockAbortException;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.logging.DeleteLogRec;
import com.exascale.logging.InsertLogRec;
import com.exascale.logging.LogRec;
import com.exascale.logging.StartLogRec;
import com.exascale.managers.BufferManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LockManager;
import com.exascale.managers.LogManager;

public class Transaction implements Serializable
{
	public static final int ISOLATION_RR = 0, ISOLATION_CS = 1, ISOLATION_UR = 2;
	private static AtomicLong nextTxNum = new AtomicLong(0);
	public static ConcurrentHashMap<Long, Long> txList = new ConcurrentHashMap<Long, Long>();

	private final long txnum;

	public int level;

	public Transaction(int level)
	{
		this.level = level;
		txnum = nextTx();
		txList.put(txnum, txnum);
		final LogRec rec = new StartLogRec(txnum);
		LogManager.write(rec);
	}

	private static long nextTx()
	{
		return nextTxNum.incrementAndGet();
	}

	public void commit() throws IOException
	{
		BufferManager.unpinAll(txnum);
		LogManager.commit(txnum);
		LockManager.release(txnum);
		txList.remove(txnum);
	}

	public DeleteLogRec delete(byte[] before, byte[] after, int off, Block b)
	{
		return LogManager.delete(txnum, b, off, before, after);
	}

	public HeaderPage forceReadHeaderPage(Block b, int type) throws LockAbortException, Exception
	{
		if (level == ISOLATION_RR || level == ISOLATION_CS)
		{
			LockManager.sLock(b, txnum);
		}
		final Page p = this.getPage(b);
		HeaderPage retval;
		p.getInt(0);
		retval = new HeaderPage(p, type);

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

	public InsertLogRec insert(byte[] before, byte[] after, int off, Block b)
	{
		return LogManager.insert(txnum, b, off, before, after);
	}

	public long number()
	{
		return txnum;
	}

	public void read(Block b, Schema schema) throws LockAbortException, Exception
	{
		if (level == ISOLATION_RR || level == ISOLATION_CS)
		{
			LockManager.sLock(b, txnum);
		}
		final Page p = this.getPage(b);
		schema.read(this, p);
		if (level == ISOLATION_CS)
		{
			LockManager.unlockSLock(b, txnum);
		}
	}

	public HeaderPage readHeaderPage(Block b, int type) throws LockAbortException, Exception
	{
		if (level == ISOLATION_RR || level == ISOLATION_CS)
		{
			LockManager.sLock(b, txnum);
		}
		final Page p = this.getPage(b);
		HeaderPage retval;
		final int first = p.getInt(0);
		if (b.number() == 0 || first != -1)
		{
			retval = new HeaderPage(p, type);
		}
		else
		{
			retval = null; // return null if this header page is not used yet
		}

		if (level == ISOLATION_CS)
		{
			LockManager.unlockSLock(b, txnum);
		}

		return retval;
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
			catch (final InterruptedException e)
			{
				continue;
			}
		}
	}

	public void requestPages(Block[] bs)
	{
		String cmd = "REQUEST PAGES " + bs.length + "~";
		for (final Block b : bs)
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
			catch (final InterruptedException e)
			{
				continue;
			}
		}
	}

	public void rollback() throws IOException
	{
		BufferManager.unpinAll(txnum);
		LogManager.rollback(txnum);
		LockManager.release(txnum);
		txList.remove(txnum);
	}

	public void setIsolationLevel(int level)
	{
		this.level = level;
	}

	public void unpin(Page p)
	{
		BufferManager.unpin(p, txnum);
	}

	public Page getPage(Block b) throws Exception
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
					Thread.sleep(Long.parseLong(HRDBMSWorker.getHParms().getProperty("getpage_fail_sleep_time_ms")));
				}
				catch (final InterruptedException e)
				{
				}

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
				catch (final InterruptedException e)
				{
				}
			}

			requests++;
		}

		if (p == null)
		{
			throw new Exception("Unable to retrieve page " + b.fileName() + ":" + b.number());
		}

		return p;
	}
}
