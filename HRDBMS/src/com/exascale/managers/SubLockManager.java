package com.exascale.managers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import com.exascale.exceptions.LockAbortException;
import com.exascale.filesystem.Block;
import com.exascale.misc.MultiHashMap;

public class SubLockManager
{
	private HashMap<Block, HashSet<Long>> sBlocksToTXs = new HashMap<Block, HashSet<Long>>();
	private HashMap<Long, HashSet<Block>> sTXsToBlocks = new HashMap<Long, HashSet<Block>>();
	private HashMap<Block, Long> xBlocksToTXs = new HashMap<Block, Long>();
	private HashMap<Long, HashSet<Block>> xTXsToBlocks = new HashMap<Long, HashSet<Block>>();
	private ReentrantLock lock = new ReentrantLock();
	private HashMap<Block, ArrayList<Thread>> waitList = new HashMap<Block, ArrayList<Thread>>();
	private static final long TIMEOUT = Long.parseLong(HRDBMSWorker.getHParms().getProperty("deadlock_timeout_secs")) * 1000;
	
	public void verifyClear()
	{
		lock.lock();
		sBlocksToTXs.clear();
		sTXsToBlocks.clear();
		xBlocksToTXs.clear();
		xTXsToBlocks.clear();
		lock.unlock();
	}
	
	public void release(long txnum)
	{
		lock.lock();
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
					if (threads.size() == 0)
					{
						waitList.remove(b);
					}
					synchronized(thread)
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
						if (threads.size() == 0)
						{
							waitList.remove(b);
						}
						synchronized(thread)
						{
							thread.notify();
						}
					}
				}
			}
		}
		
		lock.unlock();
	}

	public void sLock(Block b, long txnum) throws LockAbortException
	{
		long start = System.currentTimeMillis();
		lock.lock();
		while (true)
		{
			Long xTx = xBlocksToTXs.get(b); //does someone have an xLock?
			if (xTx != null)
			{
				if (xTx.longValue() != txnum)
				{
					HRDBMSWorker.logger.debug("Can't get sLock on " + b + " for transaction " + txnum + " because " + xTx + " has an xLock");
					long current = System.currentTimeMillis();
					if (current - start >= TIMEOUT)
					{
						lock.unlock();
						HRDBMSWorker.logger.warn("Timed out trying to obtain sLock on " + b);
						throw new LockAbortException();
					}
					
					long myTimeout = TIMEOUT - (current - start);
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
					
					lock.unlock();
					synchronized(Thread.currentThread())
					{
						try
						{
							Thread.currentThread().wait(myTimeout);
						}
						catch(Exception e)
						{}
					}
					
					lock.lock();
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
			if (threads.size() == 0)
			{
				waitList.remove(b);
			}
			synchronized(thread)
			{
				thread.notify();
			}
		}
		
		lock.unlock();
	}

	public void xLock(Block b, long txnum) throws LockAbortException
	{
		long start = System.currentTimeMillis();
		lock.lock();
		while (true)
		{
			Long tx = xBlocksToTXs.get(b);
			if (tx != null)
			{
				if (tx.longValue() != txnum)
				{
					HRDBMSWorker.logger.debug("Can't get xLock on " + b + " for transaction " + txnum + " because " + tx + " has an xLock");
					
					long current = System.currentTimeMillis();
					if (current - start >= TIMEOUT)
					{
						lock.unlock();
						HRDBMSWorker.logger.warn("Timed out trying to obtain xLock on " + b);
						throw new LockAbortException();
					}
					
					long myTimeout = TIMEOUT - (current - start);
				
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
				
					lock.unlock();
					synchronized(Thread.currentThread())
					{
						try
						{
							Thread.currentThread().wait(myTimeout);
						}
						catch(Exception e)
						{}
					}
				
					lock.lock();
				}
				else
				{
					lock.unlock();
					return;
				}
			}
			else
			{
				//also check that there are no sLocks, except for possibly myself
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
						
						long current = System.currentTimeMillis();
						if (current - start >= TIMEOUT)
						{
							lock.unlock();
							HRDBMSWorker.logger.warn("Timed out trying to obtain xLock on " + b);
							throw new LockAbortException();
						}
						
						long myTimeout = TIMEOUT - (current - start);
					
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
					
						lock.unlock();
						synchronized(Thread.currentThread())
						{
							try
							{
								Thread.currentThread().wait(myTimeout);
							}
							catch(Exception e)
							{}
						}
					
						lock.lock();
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
