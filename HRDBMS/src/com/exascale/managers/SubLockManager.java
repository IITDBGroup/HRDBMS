package com.exascale.managers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;
import com.exascale.exceptions.LockAbortException;
import com.exascale.filesystem.Block;

public class SubLockManager
{
	public HashMap<Block, HashSet<Long>> sBlocksToTXs = new HashMap<Block, HashSet<Long>>();
	private final HashMap<Long, HashSet<Block>> sTXsToBlocks = new HashMap<Long, HashSet<Block>>();
	public HashMap<Block, Long> xBlocksToTXs = new HashMap<Block, Long>();
	private final HashMap<Long, HashSet<Block>> xTXsToBlocks = new HashMap<Long, HashSet<Block>>();
	public ReentrantLock lock = new ReentrantLock();
	public HashMap<Block, ArrayList<Thread>> waitList = new HashMap<Block, ArrayList<Thread>>();
	public HashMap<Thread, Block> inverseWaitList = new HashMap<Thread, Block>();
	public HashMap<Thread, Long> threads2Txs = new HashMap<Thread, Long>();
	public HashMap<Long, Thread> txs2Threads = new HashMap<Long, Thread>();

	// private static final long TIMEOUT =
	// Long.parseLong(HRDBMSWorker.getHParms().getProperty("deadlock_timeout_secs"))
	// * 1000;

	public void release(long txnum)
	{
		lock.lock();
		try
		{
			HashSet<Block> array = xTXsToBlocks.remove(txnum);

			if (array != null)
			{
				for (Block b : array)
				{
					xBlocksToTXs.remove(b);
					ArrayList<Thread> threads = waitList.get(b);
					if (threads != null)
					{
						Thread thread = threads.remove(0);
						inverseWaitList.remove(thread);
						Long tx = threads2Txs.remove(thread);
						txs2Threads.remove(tx);
						if (threads.size() == 0)
						{
							waitList.remove(b);
						}
						synchronized (thread)
						{
							thread.notify();
						}
					}
				}
			}

			array = sTXsToBlocks.remove(txnum);

			if (array != null)
			{
				for (Block b : array)
				{
					HashSet<Long> array2 = sBlocksToTXs.get(b);
					array2.remove(txnum);
					if (array2.size() == 0)
					{
						sBlocksToTXs.remove(b);
						ArrayList<Thread> threads = waitList.get(b);
						if (threads != null)
						{
							Thread thread = threads.remove(0);
							inverseWaitList.remove(thread);
							Long tx = threads2Txs.remove(thread);
							txs2Threads.remove(tx);
							if (threads.size() == 0)
							{
								waitList.remove(b);
							}
							synchronized (thread)
							{
								thread.notify();
							}
						}
					}
				}
			}
		}
		catch (Exception e)
		{
			lock.unlock();
			return;
		}

		lock.unlock();
	}

	public void sLock(Block b, long txnum) throws LockAbortException
	{
		// long start = System.currentTimeMillis();
		lock.lock();
		HashSet<Long> set = sBlocksToTXs.get(b);
		if (set != null && set.contains(txnum))
		{
			lock.unlock();
			return;
		}

		while (true)
		{
			Long xTx = xBlocksToTXs.get(b); // does someone have an xLock?
			if (xTx != null)
			{
				if (xTx.longValue() != txnum)
				{
					HRDBMSWorker.logger.debug("Can't get sLock on " + b + " for transaction " + txnum + " because " + xTx + " has an xLock");
					// long current = System.currentTimeMillis();
					// if (current - start >= TIMEOUT)
					// {
					// lock.unlock();
					// HRDBMSWorker.logger.warn("Timed out trying to obtain
					// sLock on "
					// + b);
					// throw new LockAbortException();
					// }

					// long myTimeout = TIMEOUT - (current - start);
					ArrayList<Thread> threads = waitList.get(b);
					if (threads == null)
					{
						threads = new ArrayList<Thread>();
						threads.add(Thread.currentThread());
						waitList.put(b, threads);
					}
					else
					{
						threads.add(Thread.currentThread());
					}

					inverseWaitList.put(Thread.currentThread(), b);
					threads2Txs.put(Thread.currentThread(), txnum);
					txs2Threads.put(txnum, Thread.currentThread());

					synchronized (Thread.currentThread())
					{
						lock.unlock();

						try
						{
							Thread.currentThread().wait();
						}
						catch (Exception e)
						{
						}
					}

					lock.lock();
					if (LockManager.killed.containsKey(Thread.currentThread()))
					{
						LockManager.killed.remove(Thread.currentThread());
						lock.unlock();
						throw new LockAbortException();
					}
				}
				else
				{
					lock.unlock();
					return;
				}
			}
			else
			{
				break;
			}
		}

		HashSet<Long> array = sBlocksToTXs.get(b);
		if (array == null)
		{
			array = new HashSet<Long>();
			array.add(txnum);
			sBlocksToTXs.put(b, array);
		}
		else
		{
			if (!array.contains(txnum))
			{
				array.add(txnum);
			}
			else
			{
				lock.unlock();
				return;
			}
		}

		HashSet<Block> array2 = sTXsToBlocks.get(txnum);
		if (array2 == null)
		{
			array2 = new HashSet<Block>();
			array2.add(b);
			sTXsToBlocks.put(txnum, array2);
		}
		else
		{
			array2.add(b);
		}

		lock.unlock();
	}

	public void unlockSLock(Block b, long txnum)
	{
		lock.lock();
		HashSet<Block> array = sTXsToBlocks.get(txnum);

		if (array == null)
		{
			lock.unlock();
			return;
		}
		else
		{
			array.remove(b);
			if (array.size() == 0)
			{
				sTXsToBlocks.remove(txnum);
			}
		}

		HashSet<Long> array2 = sBlocksToTXs.get(b);
		if (array2 != null)
		{
			array2.remove(txnum);
			if (array2.size() == 0)
			{
				sBlocksToTXs.remove(b);
			}
		}

		ArrayList<Thread> threads = waitList.get(b);
		if (threads != null)
		{
			Thread thread = threads.remove(0);
			inverseWaitList.remove(thread);
			Long tx = threads2Txs.remove(thread);
			txs2Threads.remove(tx);
			if (threads.size() == 0)
			{
				waitList.remove(b);
			}
			synchronized (thread)
			{
				thread.notify();
			}
		}

		lock.unlock();
	}

	/*
	 * public void releaseUnusedXLocks(Transaction tx, Index index) {
	 * lock.lock(); HashSet<Block> xlocks = xTXsToBlocks.get(tx.number());
	 *
	 * if (xlocks != null) { for (Block block : (HashSet<Block>)xlocks.clone())
	 * { if (!index.changedBlocks.contains(block)) { unlockXLock(block,
	 * tx.number()); index.myPages.remove(block); } } }
	 *
	 * lock.unlock(); }
	 *
	 * public void unlockXLock(Block b, long txnum) { lock.lock();
	 * HashSet<Block> array = xTXsToBlocks.get(txnum);
	 *
	 * if (array == null) { lock.unlock(); return; } else { array.remove(b); if
	 * (array.size() == 0) { xTXsToBlocks.remove(txnum); } }
	 *
	 * xBlocksToTXs.remove(b);
	 *
	 * ArrayList<Thread> threads = waitList.get(b); if (threads != null) {
	 * Thread thread = threads.remove(0); inverseWaitList.remove(thread); Long
	 * tx = threads2Txs.remove(thread); txs2Threads.remove(tx); if
	 * (threads.size() == 0) { waitList.remove(b); } synchronized(thread) {
	 * thread.notify(); } }
	 *
	 * lock.unlock(); }
	 */

	public void verifyClear()
	{
		lock.lock();
		try
		{
			sBlocksToTXs.clear();
			sTXsToBlocks.clear();
			xBlocksToTXs.clear();
			xTXsToBlocks.clear();
		}
		catch (Exception e)
		{
			lock.unlock();
			return;
		}
		lock.unlock();
	}

	public void xLock(Block b, long txnum) throws LockAbortException
	{
		// long start = System.currentTimeMillis();
		lock.lock();
		Long set = xBlocksToTXs.get(b);
		if (set != null && set.longValue() == txnum)
		{
			lock.unlock();
			return;
		}

		while (true)
		{
			Long tx = xBlocksToTXs.get(b);
			if (tx != null)
			{
				if (tx.longValue() != txnum)
				{
					HRDBMSWorker.logger.debug("Can't get xLock on " + b + " for transaction " + txnum + " because " + tx + " has an xLock");

					// long current = System.currentTimeMillis();
					// if (current - start >= TIMEOUT)
					// {
					// lock.unlock();
					// HRDBMSWorker.logger.warn("Timed out trying to obtain
					// xLock on "
					// + b);
					// throw new LockAbortException();
					// }

					// long myTimeout = TIMEOUT - (current - start);

					ArrayList<Thread> threads = waitList.get(b);
					if (threads == null)
					{
						threads = new ArrayList<Thread>();
						threads.add(Thread.currentThread());
						waitList.put(b, threads);
					}
					else
					{
						threads.add(Thread.currentThread());
					}

					inverseWaitList.put(Thread.currentThread(), b);
					threads2Txs.put(Thread.currentThread(), txnum);
					txs2Threads.put(txnum, Thread.currentThread());

					synchronized (Thread.currentThread())
					{
						lock.unlock();

						try
						{
							Thread.currentThread().wait();
						}
						catch (Exception e)
						{
						}
					}

					lock.lock();
					if (LockManager.killed.containsKey(Thread.currentThread()))
					{
						LockManager.killed.remove(Thread.currentThread());
						lock.unlock();
						throw new LockAbortException();
					}
				}
				else
				{
					lock.unlock();
					return;
				}
			}
			else
			{
				// also check that there are no sLocks, except for possibly
				// myself
				HashSet<Long> txs = sBlocksToTXs.get(b);
				if (txs == null)
				{
					break;
				}
				else
				{
					if (txs.size() == 1 && txs.contains(txnum))
					{
						break;
					}
					else
					{
						HRDBMSWorker.logger.debug("Can't get xLock on " + b + " for transaction " + txnum + " because " + txs + " have sLocks");

						// long current = System.currentTimeMillis();
						// if (current - start >= TIMEOUT)
						// {
						// lock.unlock();
						// HRDBMSWorker.logger.warn("Timed out trying to obtain
						// xLock on "
						// + b);
						// throw new LockAbortException();
						// }

						// long myTimeout = TIMEOUT - (current - start);

						ArrayList<Thread> threads = waitList.get(b);
						if (threads == null)
						{
							threads = new ArrayList<Thread>();
							threads.add(Thread.currentThread());
							waitList.put(b, threads);
						}
						else
						{
							threads.add(Thread.currentThread());
						}

						inverseWaitList.put(Thread.currentThread(), b);
						threads2Txs.put(Thread.currentThread(), txnum);
						txs2Threads.put(txnum, Thread.currentThread());

						synchronized (Thread.currentThread())
						{
							lock.unlock();

							try
							{
								Thread.currentThread().wait();
							}
							catch (Exception e)
							{
							}
						}

						lock.lock();
						if (LockManager.killed.containsKey(Thread.currentThread()))
						{
							LockManager.killed.remove(Thread.currentThread());
							lock.unlock();
							throw new LockAbortException();
						}
					}
				}
			}
		}

		xBlocksToTXs.put(b, txnum);
		HashSet<Block> array = xTXsToBlocks.get(txnum);
		if (array == null)
		{
			array = new HashSet<Block>();
			array.add(b);
			xTXsToBlocks.put(txnum, array);
		}
		else
		{
			if (!array.contains(b))
			{
				array.add(b);
			}
		}

		lock.unlock();
	}
}
