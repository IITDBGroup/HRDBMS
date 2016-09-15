package com.exascale.tables;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import com.exascale.exceptions.LockAbortException;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.logging.DeleteLogRec;
import com.exascale.logging.InsertLogRec;
import com.exascale.logging.LogRec;
import com.exascale.logging.StartLogRec;
import com.exascale.logging.TruncateLogRec;
import com.exascale.managers.BufferManager;
import com.exascale.managers.BufferManager.RequestPagesThread;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LockManager;
import com.exascale.managers.LogManager;
import com.exascale.managers.SubBufferManager;
import com.exascale.misc.ScalableStampedReentrantRWLock;
import com.exascale.optimizer.MetaData;

public class Transaction implements Serializable
{
	public static final int ISOLATION_RR = 0, ISOLATION_CS = 1, ISOLATION_UR = 2;
	private static AtomicLong nextTxNum;
	public static ConcurrentHashMap<Long, Long> txList = new ConcurrentHashMap<Long, Long>();
	public static final boolean reorder = true;

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

	public static Object txListLock = new Object();

	public static WeakHashMap<Transaction, ConcurrentHashMap<String, HashMap<Integer, Integer>>> colMaps = new WeakHashMap<Transaction, ConcurrentHashMap<String, HashMap<Integer, Integer>>>();

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
		//Transaction.txListLock.lock();
		synchronized(txListLock)
		{
			txList.put(txnum, txnum);
		}
		//Transaction.txListLock.unlock();
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
		this.txnum = txnum; // no read lock needed?
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
		for (SubBufferManager sbm : BufferManager.managers)
		{
			sbm.lock.lock();
		}
		//Transaction.txListLock.lock();
		synchronized(txListLock)
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
				//Transaction.txListLock.unlock();
				for (SubBufferManager sbm : BufferManager.managers)
				{
					sbm.lock.unlock();
				}
				throw e;
			}
		}
		//Transaction.txListLock.unlock();
		for (SubBufferManager sbm : BufferManager.managers)
		{
			sbm.trimBP();
			sbm.lock.unlock();
		}
	}

	public void commitNoFlush() throws Exception
	{
		LogManager.commitNoFlush(txnum);
		for (SubBufferManager sbm : BufferManager.managers)
		{
			sbm.lock.lock();
		}
		//Transaction.txListLock.lock();
		synchronized(txListLock)
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
				//Transaction.txListLock.unlock();
				for (SubBufferManager sbm : BufferManager.managers)
				{
					sbm.lock.unlock();
				}
				throw e;
			}
		}
		//Transaction.txListLock.unlock();
		for (SubBufferManager sbm : BufferManager.managers)
		{
			sbm.trimBP();
			sbm.lock.unlock();
		}
	}

	public DeleteLogRec delete(byte[] before, byte[] after, int off, Block b) throws Exception
	{
		if (!txList.containsKey(txnum))
		{
			//Transaction.txListLock.lock();
			synchronized(txListLock)
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
					//Transaction.txListLock.unlock();
					throw e;
				}
			}
			//Transaction.txListLock.unlock();
		}
		return LogManager.delete(txnum, b, off, before, after);
	}

	public void dummyRead(Block b, Schema schema) throws LockAbortException, Exception
	{
		final Page p = this.getPage(b);
		schema.dummyRead(this, p);
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

	public HashMap<Integer, Integer> getColOrder(Block bl) throws Exception
	{
		final Block b = new Block(bl.fileName(), 0);
		requestPage(b);

		final HeaderPage hp = readHeaderPage(b, 1);
		ArrayList<Integer> order = hp.getColOrder();
		HashMap<Integer, Integer> retval = new HashMap<Integer, Integer>();
		int index = 0;
		for (int i : order)
		{
			retval.put(i, index);
			index++;
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

		// HRDBMSWorker.logger.debug("Trying to get block: " + b);
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
			BufferManager.requestPage(b, this.number());
		}

		p = BufferManager.getPage(b, txnum);

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

	public Page[] getPage(Block b, ArrayList<Integer> cols) throws Exception
	{
		Page[] retval = new Page[cols.size()];

		if (!reorder)
		{
			int pos = 0;
			for (int col : cols)
			{
				retval[pos++] = getPage(new Block(b.fileName(), b.number() + col));
			}
		}
		else
		{
			ConcurrentHashMap<String, HashMap<Integer, Integer>> colMap = null;
			synchronized(colMaps)
			{
				colMap = colMaps.get(this);
				if (colMap == null)
				{
					colMap = new ConcurrentHashMap<String, HashMap<Integer, Integer>>();
					colMaps.put(this, colMap);
				}
			}
			HashMap<Integer, Integer> map = colMap.get(b.fileName());
			if (map == null)
			{
				map = getColOrder(b);
				colMap.put(b.fileName(), map);
			}
			int pos = 0;
			for (int col : cols)
			{
				retval[pos++] = getPage(new Block(b.fileName(), b.number() + map.get(col)));
			}
		}

		return retval;
	}

	@Override
	public int hashCode()
	{
		return Long.valueOf(txnum).hashCode();
	}

	public InsertLogRec insert(byte[] before, byte[] after, int off, Block b) throws Exception
	{
		if (!txList.containsKey(txnum))
		{
			//Transaction.txListLock.lock();
			synchronized(txListLock)
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
					//Transaction.txListLock.unlock();
					throw e;
				}
			}
			//Transaction.txListLock.unlock();
		}
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

	public void read(Block b, Schema schema, ArrayList<Integer> cols, boolean forIter) throws Exception
	{
		if (!reorder)
		{
			for (int col : cols)
			{
				Block b2 = new Block(b.fileName(), b.number() + col);
				if (level == ISOLATION_RR || level == ISOLATION_CS)
				{
					LockManager.sLock(b2, txnum);
				}
				final Page p = this.getPage(b2);
				schema.add(col, p);
			}
		}
		else
		{
			ConcurrentHashMap<String, HashMap<Integer, Integer>> colMap = null;
			synchronized(colMaps)
			{
				colMap = colMaps.get(this);
				if (colMap == null)
				{
					colMap = new ConcurrentHashMap<String, HashMap<Integer, Integer>>();
					colMaps.put(this, colMap);
				}
			}
			HashMap<Integer, Integer> map = colMap.get(b.fileName());
			if (map == null)
			{
				map = getColOrder(b);
				colMap.put(b.fileName(), map);
			}
			for (int col : cols)
			{
				Block b2 = new Block(b.fileName(), b.number() + map.get(col));
				if (level == ISOLATION_RR || level == ISOLATION_CS)
				{
					LockManager.sLock(b2, txnum);
				}
				final Page p = this.getPage(b2);
				schema.add(col, p);
			}
		}

		if (forIter)
		{
			schema.read(this, true);
		}
		else
		{
			schema.read(this);
		}
	}

	public void read(Block b, Schema schema, ArrayList<Integer> cols, boolean forIter, boolean lock) throws Exception
	{
		if (!reorder)
		{
			for (int col : cols)
			{
				Block b2 = new Block(b.fileName(), b.number() + col);
				LockManager.xLock(b2, txnum);
				final Page p = this.getPage(b2);
				schema.add(col, p);
			}
		}
		else
		{
			ConcurrentHashMap<String, HashMap<Integer, Integer>> colMap = null;
			synchronized(colMaps)
			{
				colMap = colMaps.get(this);
				if (colMap == null)
				{
					colMap = new ConcurrentHashMap<String, HashMap<Integer, Integer>>();
					colMaps.put(this, colMap);
				}
			}
			HashMap<Integer, Integer> map = colMap.get(b.fileName());
			if (map == null)
			{
				map = getColOrder(b);
				colMap.put(b.fileName(), map);
			}
			for (int col : cols)
			{
				Block b2 = new Block(b.fileName(), b.number() + map.get(col));
				LockManager.xLock(b2, txnum);
				final Page p = this.getPage(b2);
				schema.add(col, p);
			}
		}

		if (forIter)
		{
			schema.read(this, true);
		}
		else
		{
			schema.read(this);
		}
	}

	public void read(Block b, Schema schema, boolean lock) throws LockAbortException, Exception
	{
		LockManager.xLock(b, txnum);
		final Page p = this.getPage(b);
		schema.read(this, p);
	}

	public void read2(Block b, Schema schema, Page p) throws LockAbortException, Exception
	{
		if (level == ISOLATION_RR || level == ISOLATION_CS)
		{
			LockManager.sLock(b, txnum);
		}
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

	public void releaseLocksAndPins() throws Exception
	{
		for (SubBufferManager sbm : BufferManager.managers)
		{
			sbm.lock.lock();
		}

		//Transaction.txListLock.lock();
		synchronized(txListLock)
		{
			try
			{
				BufferManager.unpinAll(txnum);
				LockManager.release(txnum);
			}
			catch (Exception e)
			{
				//Transaction.txListLock.unlock();
				for (SubBufferManager sbm : BufferManager.managers)
				{
					sbm.lock.unlock();
				}
				throw e;
			}
		}
		//Transaction.txListLock.unlock();
		for (SubBufferManager sbm : BufferManager.managers)
		{
			sbm.lock.unlock();
		}
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

	public void requestPage(Block b, ArrayList<Integer> cols) throws Exception
	{
		TreeSet<Integer> pages = new TreeSet<Integer>();
		if (!reorder)
		{
			for (int col : cols)
			{
				// BufferManager.requestPage(new Block(b.fileName(), b.number()
				// + col), this.number());
				pages.add(b.number() + col);
			}
		}
		else
		{
			ConcurrentHashMap<String, HashMap<Integer, Integer>> colMap = null;
			synchronized(colMaps)
			{
				colMap = colMaps.get(this);
				if (colMap == null)
				{
					colMap = new ConcurrentHashMap<String, HashMap<Integer, Integer>>();
					colMaps.put(this, colMap);
				}
			}
			HashMap<Integer, Integer> map = colMap.get(b.fileName());
			if (map == null)
			{
				map = getColOrder(b);
				colMap.put(b.fileName(), map);
			}
			for (int col : cols)
			{
				// BufferManager.requestPage(new Block(b.fileName(), b.number()
				// + map.get(col)), this.number());
				pages.add(b.number() + map.get(col));
			}
		}

		ArrayList<Integer> set = new ArrayList<Integer>();
		for (int page : pages)
		{
			if (set.size() == 0)
			{
				set.add(page);
			}
			else
			{
				int prev = set.get(set.size() - 1);
				if (page == prev + 1)
				{
					set.add(page);
				}
				else
				{
					BufferManager.requestConsecutivePages(new Block(b.fileName(), set.get(0)), this.number(), set.size());
					set = new ArrayList<Integer>();
					set.add(page);
				}
			}
		}

		if (set.size() != 0)
		{
			BufferManager.requestConsecutivePages(new Block(b.fileName(), set.get(0)), this.number(), set.size());
		}
	}

	public void requestPages(Block[] bs) throws Exception
	{
		// HRDBMSWorker.logger.debug("Short TX request pages starting");
		ArrayList<Block> bs1 = new ArrayList<Block>();
		ArrayList<Block> bs2 = new ArrayList<Block>();
		int i = 0;
		while (i < bs.length)
		{
			Block b = bs[i];
			int num = b.number();
			if (num % 3 == 0)
			{
				if (i + 2 < bs.length && bs[i + 1].number() == num + 1 && bs[i + 2].number() == num + 2)
				{
					bs2.add(b);
					i += 3;
				}
				else
				{
					bs1.add(b);
					i++;
				}
			}
			else
			{
				bs1.add(b);
				i++;
			}
		}

		if (bs1.size() > 0)
		{
			Block[] b1 = bs1.toArray(new Block[bs1.size()]);
			BufferManager.requestPages(b1, this.number());
		}

		if (bs2.size() > 0)
		{
			Block[] b2 = bs2.toArray(new Block[bs2.size()]);
			BufferManager.request3Pages(b2, this.number());
		}

		// HRDBMSWorker.logger.debug("Short TX request pages ending");
	}

	public RequestPagesThread requestPages(Block[] bs, ArrayList<Integer> cols) throws Exception
	{
		Block[] bs2 = new Block[bs.length * cols.size()];
		int pos = 0;
		for (Block b : bs)
		{
			if (!reorder)
			{
				for (int col : cols)
				{
					bs2[pos++] = new Block(b.fileName(), b.number() + col);
				}
			}
			else
			{
				ConcurrentHashMap<String, HashMap<Integer, Integer>> colMap = null;
				synchronized(colMaps)
				{
					colMap = colMaps.get(this);
					if (colMap == null)
					{
						colMap = new ConcurrentHashMap<String, HashMap<Integer, Integer>>();
						colMaps.put(this, colMap);
					}
				}
				HashMap<Integer, Integer> map = colMap.get(b.fileName());
				if (map == null)
				{
					map = getColOrder(b);
					colMap.put(b.fileName(), map);
				}
				for (int col : cols)
				{
					bs2[pos++] = new Block(b.fileName(), b.number() + map.get(col));
				}
			}
		}

		return BufferManager.requestPages(bs2, this.number());
	}

	public RequestPagesThread requestPages(Block[] bs, ArrayList<Integer> cols, int layoutSize) throws Exception
	{
		ArrayList<Integer> newCols = null;
		boolean build = false;
		Block[] bs2 = new Block[bs.length * cols.size()];
		int pos = 0;
		for (Block b : bs)
		{
			if (!reorder)
			{
				for (int col : cols)
				{
					bs2[pos++] = new Block(b.fileName(), b.number() + col);
				}
			}
			else
			{
				if (newCols == null)
				{
					newCols = new ArrayList<Integer>();
					build = true;
				}
				else
				{
					build = false;
				}
				ConcurrentHashMap<String, HashMap<Integer, Integer>> colMap = null;
				synchronized(colMaps)
				{
					colMap = colMaps.get(this);
					if (colMap == null)
					{
						colMap = new ConcurrentHashMap<String, HashMap<Integer, Integer>>();
						colMaps.put(this, colMap);
					}
				}
				HashMap<Integer, Integer> map = colMap.get(b.fileName());
				if (map == null)
				{
					map = getColOrder(b);
					colMap.put(b.fileName(), map);
				}
				for (int col : cols)
				{
					int newCol = map.get(col);
					bs2[pos++] = new Block(b.fileName(), b.number() + newCol);

					if (build)
					{
						newCols.add(newCol);
					}
				}
			}
		}

		TreeSet<Integer> pages = new TreeSet<Integer>();
		for (Block b : bs2)
		{
			pages.add(b.number());
		}

		ArrayList<Integer> set = new ArrayList<Integer>();
		int rank = 0;
		ArrayList<RequestPagesThread> threads = new ArrayList<RequestPagesThread>();
		for (int page : pages)
		{
			if (set.size() == 0)
			{
				set.add(page);
			}
			else
			{
				int prev = set.get(set.size() - 1);
				if (page == prev + 1)
				{
					set.add(page);
				}
				else
				{
					if (!reorder)
					{
						threads.add(BufferManager.requestConsecutivePages(new Block(bs2[0].fileName(), set.get(0)), this.number(), set.size(), cols, layoutSize, rank));
					}
					else
					{
						threads.add(BufferManager.requestConsecutivePages(new Block(bs2[0].fileName(), set.get(0)), this.number(), set.size(), newCols, layoutSize, rank));
					}
					set = new ArrayList<Integer>();
					set.add(page);
					rank++;
				}
			}
		}

		if (set.size() != 0)
		{
			if (!reorder)
			{
				threads.add(BufferManager.requestConsecutivePages(new Block(bs2[0].fileName(), set.get(0)), this.number(), set.size(), cols, layoutSize, rank));
			}
			else
			{
				threads.add(BufferManager.requestConsecutivePages(new Block(bs2[0].fileName(), set.get(0)), this.number(), set.size(), newCols, layoutSize, rank));
			}
		}

		// if (!reorder)
		// {
		// return BufferManager.requestPages(bs2, this.number(), cols,
		// layoutSize);
		// }
		// else
		// {
		// return BufferManager.requestPages(bs2, this.number(), newCols,
		// layoutSize);
		// }
		RequestPagesThread retval = new BufferManager.RequestPagesThread(threads);
		retval.start();
		return retval;
	}

	public void requestPages(Block[] bs, Schema[] schemas, int schemaIndex, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos) throws Exception
	{
		// BufferManager.requestPages(bs, this, schemas, schemaIndex, schemaMap,
		// fetchPos);
		// HRDBMSWorker.logger.debug("Long TX request pages starting");
		ArrayList<Block> bs1 = new ArrayList<Block>();
		ArrayList<Block> bs2 = new ArrayList<Block>();
		int i = 0;
		while (i < bs.length)
		{
			Block b = bs[i];
			int num = b.number();
			if (num % 3 == 0)
			{
				if (i + 2 < bs.length && bs[i + 1].number() == num + 1 && bs[i + 2].number() == num + 2)
				{
					bs2.add(b);
					i += 3;
				}
				else
				{
					bs1.add(b);
					i++;
				}
			}
			else
			{
				bs1.add(b);
				i++;
			}
		}

		if (bs1.size() > 0)
		{
			Block[] b1 = bs1.toArray(new Block[bs1.size()]);
			BufferManager.requestPages(b1, this, schemas, schemaIndex, schemaMap, fetchPos);
		}

		if (bs2.size() > 0)
		{
			Block[] b2 = bs2.toArray(new Block[bs2.size()]);
			BufferManager.request3Pages(b2, this, schemas, schemaIndex + bs1.size(), schemaMap, fetchPos);
		}

		// HRDBMSWorker.logger.debug("Long TX request pages ending");
	}

	public void rollback() throws Exception
	{
		for (SubBufferManager sbm : BufferManager.managers)
		{
			sbm.lock.lock();
		}
		//Transaction.txListLock.lock();
		synchronized(txListLock)
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
				//Transaction.txListLock.unlock();
				for (SubBufferManager sbm : BufferManager.managers)
				{
					sbm.lock.unlock();
				}
				throw e;
			}
		}
		//Transaction.txListLock.unlock();
		for (SubBufferManager sbm : BufferManager.managers)
		{
			sbm.lock.unlock();
		}
	}

	public void setIsolationLevel(int level)
	{
		this.level = level;
	}

	public TruncateLogRec truncate(Block b) throws Exception
	{
		if (!txList.containsKey(txnum))
		{
			//Transaction.txListLock.lock();
			synchronized(txListLock)
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
					//Transaction.txListLock.unlock();
					throw e;
				}
			}
			//Transaction.txListLock.unlock();
		}
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
