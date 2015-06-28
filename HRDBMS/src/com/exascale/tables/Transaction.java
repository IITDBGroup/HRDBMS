package com.exascale.tables;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.exascale.exceptions.LockAbortException;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.logging.DeleteLogRec;
import com.exascale.logging.InsertLogRec;
import com.exascale.logging.LogRec;
import com.exascale.logging.StartLogRec;
import com.exascale.logging.TruncateLogRec;
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
		catch (Exception e)
		{
			try
			{
				HRDBMSWorker.logger.debug("", e);
			}
			catch (Exception f)
			{
			}
		}
	}

	public static ReentrantReadWriteLock txListLock = new ReentrantReadWriteLock();

	private final long txnum;
	public int level;

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
		Transaction.txListLock.writeLock().lock();
		{
			txList.put(txnum, txnum);
		}
		Transaction.txListLock.writeLock().unlock();
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

	public Transaction(long txnum)
	{
		Transaction.txListLock.readLock().lock();
		{
			this.txnum = txnum;
		}
		Transaction.txListLock.readLock().unlock();

		level = Transaction.ISOLATION_RR;
	}

	private static long nextTx()
	{
		return nextTxNum.getAndAdd(Integer.parseInt(HRDBMSWorker.getHParms().getProperty("number_of_coords")));
	}

	public void checkpoint() throws Exception
	{
		BufferManager.unpinAll(txnum);
	}

	public void checkpoint(int lt, String tFn) throws Exception
	{
		// LogManager.commit(txnum);
		// synchronized(txList)
		// {
		// HRDBMSWorker.logger.debug("COMMIT: " + txnum);
		BufferManager.unpinAllLessThan(txnum, lt, tFn);
		// LockManager.release(txnum);
		// txList.remove(txnum);
		// }
	}

	public void checkpoint(int lt, String tFn, boolean flag) throws Exception
	{
		// LogManager.commit(txnum);
		// synchronized(txList)
		// {
		// HRDBMSWorker.logger.debug("COMMIT: " + txnum);
		BufferManager.unpinAllLessThan(txnum, lt, tFn, flag);
		// LockManager.release(txnum);
		// txList.remove(txnum);
		// }
	}

	public void checkpoint(String prefix) throws Exception
	{
		BufferManager.unpinMyDevice(txnum, prefix);
	}

	public void checkpoint(String iFn, Block inUse) throws Exception
	{
		BufferManager.unpinAllExcept(txnum, iFn, inUse);
	}

	public void commit() throws Exception
	{
		LogManager.commit(txnum);
		Transaction.txListLock.writeLock().lock();
		{
			try
			{
				// HRDBMSWorker.logger.debug("COMMIT: " + txnum);
				BufferManager.unpinAll(txnum);
				LockManager.release(txnum);
				txList.remove(txnum);
			}
			catch (Exception e)
			{
				Transaction.txListLock.writeLock().unlock();
				throw e;
			}
		}
		Transaction.txListLock.writeLock().unlock();
	}

	public void commitNoFlush() throws Exception
	{
		LogManager.commitNoFlush(txnum);
		Transaction.txListLock.writeLock().lock();
		{
			try
			{
				// HRDBMSWorker.logger.debug("COMMIT: " + txnum);
				BufferManager.unpinAll(txnum);
				LockManager.release(txnum);
				txList.remove(txnum);
			}
			catch (Exception e)
			{
				Transaction.txListLock.writeLock().unlock();
				throw e;
			}
		}
		Transaction.txListLock.writeLock().unlock();
	}

	public DeleteLogRec delete(byte[] before, byte[] after, int off, Block b) throws Exception
	{
		Transaction.txListLock.writeLock().lock();
		{
			try
			{
				if (!txList.containsKey(txnum))
				{
					txList.put(txnum, txnum);
					LogRec rec = new StartLogRec(txnum);
					LogManager.write(rec);
				}
			}
			catch (Exception e)
			{
				Transaction.txListLock.writeLock().unlock();
				throw e;
			}
		}
		Transaction.txListLock.writeLock().unlock();
		return LogManager.delete(txnum, b, off, before, after);
	}

	@Override
	public boolean equals(Object rhs)
	{
		if (rhs == null || !(rhs instanceof Transaction))
		{
			return false;
		}

		Transaction tx = (Transaction)rhs;
		return txnum == tx.txnum;
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

	public Page getPage(Block b) throws Exception
	{
		if (b.number() < 0)
		{
			Exception e = new Exception("Negative block number requested: " + b.number());
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
		Page p = BufferManager.getPage(b, txnum);

		if (p == null)
		{
			int sleeps = 0;
			final int attempts = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("getpage_attempts"));
			while (p == null && sleeps < attempts)
			{
				try
				{
					Thread.sleep(Long.parseLong(HRDBMSWorker.getHParms().getProperty("getpage_fail_sleep_time_ms")));
				}
				catch (final InterruptedException e)
				{
				}

				p = BufferManager.getPage(b, txnum);
				sleeps++;
			}
		}

		if (p == null)
		{
			throw new Exception("Unable to retrieve page " + b.fileName() + ":" + b.number());
		}

		return p;
	}

	@Override
	public int hashCode()
	{
		return Long.valueOf(txnum).hashCode();
	}

	public InsertLogRec insert(byte[] before, byte[] after, int off, Block b) throws Exception
	{
		Transaction.txListLock.writeLock().lock();
		{
			try
			{
				if (!txList.containsKey(txnum))
				{
					txList.put(txnum, txnum);
					LogRec rec = new StartLogRec(txnum);
					LogManager.write(rec);
				}
			}
			catch (Exception e)
			{
				Transaction.txListLock.writeLock().unlock();
				throw e;
			}
		}
		Transaction.txListLock.writeLock().unlock();
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
	
	public void dummyRead(Block b, Schema schema) throws LockAbortException, Exception
	{
		final Page p = this.getPage(b);
		schema.dummyRead(this, p);
	}

	public void read(Block b, Schema schema, boolean lock) throws LockAbortException, Exception
	{
		LockManager.xLock(b, txnum);
		final Page p = this.getPage(b);
		schema.read(this, p);
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

	public void releaseLocksAndPins() throws Exception
	{
		Transaction.txListLock.writeLock().lock();
		{
			try
			{
				BufferManager.unpinAll(txnum);
				LockManager.release(txnum);
			}
			catch (Exception e)
			{
				Transaction.txListLock.writeLock().unlock();
				throw e;
			}
		}
		Transaction.txListLock.writeLock().unlock();
	}

	public void requestPage(Block b) throws Exception
	{

		if (b.number() < 0)
		{
			Exception e = new Exception("Negative block number requested: " + b.number());
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
				Exception e = new Exception("Negative block number requested: " + b.number());
				HRDBMSWorker.logger.debug("", e);
				throw e;
			}
		}

		BufferManager.requestPages(bs, this.number());
	}

	public void rollback() throws Exception
	{
		Transaction.txListLock.writeLock().lock();
		{
			try
			{
				if (!txList.containsKey(txnum))
				{
					txList.put(txnum, txnum);
					LogRec rec = new StartLogRec(txnum);
					LogManager.write(rec);
				}
				HRDBMSWorker.logger.debug("ROLLBACK: " + txnum);
				LogManager.rollback(txnum);
				BufferManager.unpinAll(txnum);
				LockManager.release(txnum);
				txList.remove(txnum);
			}
			catch (Exception e)
			{
				Transaction.txListLock.writeLock().unlock();
				throw e;
			}
		}
		Transaction.txListLock.writeLock().unlock();
	}

	public void setIsolationLevel(int level)
	{
		this.level = level;
	}

	public TruncateLogRec truncate(Block b) throws Exception
	{
		Transaction.txListLock.writeLock().lock();
		{
			try
			{
				if (!txList.containsKey(txnum))
				{
					txList.put(txnum, txnum);
					LogRec rec = new StartLogRec(txnum);
					LogManager.write(rec);
				}
			}
			catch (Exception e)
			{
				Transaction.txListLock.writeLock().unlock();
				throw e;
			}
		}
		Transaction.txListLock.writeLock().unlock();
		return LogManager.truncate(txnum, b);
	}

	public void tryCommit(String host) throws IOException
	{
		try
		{
			LogManager.ready(txnum, host);
		}
		catch (Exception e)
		{
			LogManager.notReady(txnum);
			throw new IOException();
		}
	}

	public void unpin(Page p)
	{
		BufferManager.unpin(p, txnum);
	}
}
