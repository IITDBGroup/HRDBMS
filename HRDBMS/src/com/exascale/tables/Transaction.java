package com.exascale.tables;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
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
import com.exascale.optimizer.MetaData;

public class Transaction implements Serializable
{
	public static final int ISOLATION_RR = 0, ISOLATION_CS = 1, ISOLATION_UR = 2;
	private static AtomicLong nextTxNum;
	public static HashMap<Long, Long> txList = new HashMap<Long, Long>();
	
	static
	{
		try
		{
			nextTxNum = new AtomicLong(System.currentTimeMillis() * 1000);
			while (nextTxNum.get() % MetaData.myCoordNum() != 0)
			{
				nextTxNum.getAndIncrement();
			}
		}
		catch(Exception e)
		{
			try
			{
				HRDBMSWorker.logger.debug("", e);
			}
			catch(Exception f)
			{}
		}
	}

	private final long txnum;

	public int level;
	
	public Transaction(long txnum)
	{
		synchronized(txList)
		{
			this.txnum = txnum;
		}
		
		level = Transaction.ISOLATION_RR;
	}

	public Transaction(int level)
	{
		if (HRDBMSWorker.type == HRDBMSWorker.TYPE_WORKER)
		{
			Exception e = new Exception();
			HRDBMSWorker.logger.fatal("A worker node asked for a new transaction number", e);
			System.exit(1);
		}
		this.level = level;
		txnum = nextTx();
		synchronized(txList)
		{
			txList.put(txnum, txnum);
		}
		LogRec rec = new StartLogRec(txnum);
		LogManager.write(rec);
		String filename = HRDBMSWorker.getHParms().getProperty("log_dir");
		if (!filename.endsWith("/"))
		{
			filename += "/";
		}
		filename += "xa.log";
		rec = new StartLogRec(txnum);
		LogManager.write(rec, filename);
	}
	
	public boolean equals(Object rhs)
	{
		if (rhs == null || !(rhs instanceof Transaction))
		{
			return false;
		}
		
		Transaction tx = (Transaction)rhs;
		return txnum == tx.txnum;
	}
	
	public int hashCode()
	{
		return Long.valueOf(txnum).hashCode();
	}

	private static long nextTx()
	{
		return nextTxNum.getAndAdd(Integer.parseInt(HRDBMSWorker.getHParms().getProperty("number_of_coords")));
	}

	public void commit() throws Exception
	{
		synchronized(txList)
		{
			if (txList.containsKey(txnum))
			{
				HRDBMSWorker.logger.debug("COMMIT: " + txnum);
				BufferManager.unpinAll(txnum);
				LogManager.commit(txnum);
				LockManager.release(txnum);
				txList.remove(txnum);
			}
		}
	}
	
	public void tryCommit(String host) throws IOException
	{
		synchronized(txList)
		{
			if (txList.containsKey(txnum))
			{
				try
				{
					LogManager.ready(txnum, host);
				}
				catch(Exception e)
				{
					LogManager.notReady(txnum);
					throw new IOException();
				}
			}
		}
	}

	public DeleteLogRec delete(byte[] before, byte[] after, int off, Block b) throws Exception
	{
		synchronized(txList)
		{
			if (!txList.containsKey(txnum))
			{
				txList.put(txnum, txnum);
				LogManager.writeStartRecIfNeeded(txnum);
			}
		}
		return LogManager.delete(txnum, b, off, before, after);
	}

	public HeaderPage forceReadHeaderPage(Block b, int type) throws LockAbortException, Exception
	{
		synchronized(txList)
		{
			if (!txList.containsKey(txnum))
			{
				txList.put(txnum, txnum);
				LogManager.writeStartRecIfNeeded(txnum);
			}
		}
		
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

	public InsertLogRec insert(byte[] before, byte[] after, int off, Block b) throws Exception
	{
		synchronized(txList)
		{
			if (!txList.containsKey(txnum))
			{
				txList.put(txnum, txnum);
				LogManager.writeStartRecIfNeeded(txnum);
			}
		}
		return LogManager.insert(txnum, b, off, before, after);
	}

	public long number()
	{
		return txnum;
	}

	public void read(Block b, Schema schema) throws LockAbortException, Exception
	{
		synchronized(txList)
		{
			if (!txList.containsKey(txnum))
			{
				txList.put(txnum, txnum);
				LogManager.writeStartRecIfNeeded(txnum);
			}
		}
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
	
	public void read(Block b, Schema schema, boolean lock) throws LockAbortException, Exception
	{
		synchronized(txList)
		{
			if (!txList.containsKey(txnum))
			{
				txList.put(txnum, txnum);
				LogManager.writeStartRecIfNeeded(txnum);
			}
		}
		
		LockManager.xLock(b, txnum);
		final Page p = this.getPage(b);
		schema.read(this, p);
	}

	public HeaderPage readHeaderPage(Block b, int type) throws LockAbortException, Exception
	{
		synchronized(txList)
		{
			if (!txList.containsKey(txnum))
			{
				txList.put(txnum, txnum);
				LogManager.writeStartRecIfNeeded(txnum);
			}
		}
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

	public void requestPage(Block b) throws Exception
	{
		
		if (b.number() < 0)
		{
			Exception e = new Exception("Negative block number requested");
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
		
		BufferManager.requestPage(b, this.number());
	}

	public void requestPages(Block[] bs) throws Exception
	{
		for (Block b : bs)
		{
			if (b.number() < 0)
			{
				Exception e = new Exception("Negative block number requested");
				HRDBMSWorker.logger.debug("", e);
				throw e;
			}
		}
		
		BufferManager.requestPages(bs, this.number());
	}

	public void rollback() throws Exception
	{
		synchronized(txList)
		{
			if (txList.containsKey(txnum))
			{
				HRDBMSWorker.logger.debug("ROLLBACK: " + txnum);
				BufferManager.unpinAll(txnum);
				LogManager.rollback(txnum);
				LockManager.release(txnum);
				txList.remove(txnum);
			}
		}
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
		if (b.number() < 0)
		{
			Exception e = new Exception("Negative block number requested");
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
		Page p = BufferManager.getPage(b);

		if (p == null)
		{
			int sleeps = 0;
			while (p == null && sleeps < Integer.parseInt(HRDBMSWorker.getHParms().getProperty("getpage_attempts")))
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
		}

		if (p == null)
		{
			throw new Exception("Unable to retrieve page " + b.fileName() + ":" + b.number());
		}

		return p;
	}
}
