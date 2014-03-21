package com.exascale.locking;

import java.util.concurrent.ConcurrentHashMap;
import com.exascale.exceptions.LockAbortException;
import com.exascale.filesystem.Block;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.MultiHashMap;

public class LockTable
{
	private static int MAX_TIME_SECS;
	private static ConcurrentHashMap<Block, Integer> locks = new ConcurrentHashMap<Block, Integer>();
	private static MultiHashMap<Block, Thread> waitList = new MultiHashMap<Block, Thread>();
	public static boolean blockSLocks = false;

	public LockTable()
	{
		MAX_TIME_SECS = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("deadlock_timeout_secs"));
	}

	// waiting on a lock can't hold a sync on the whole lock table
	public static void sLock(Block b) throws LockAbortException
	{
		while (blockSLocks)
		{
			try
			{
				Thread.sleep(Long.parseLong(HRDBMSWorker.getHParms().getProperty("slock_block_sleep_ms")));
			}
			catch (final InterruptedException e)
			{
			}
		}

		try
		{
			final long time = System.currentTimeMillis();
			while (true)
			{
				while (hasXLock(b) && !waitingTooLong(time))
				{
					waitList.multiPut(b, Thread.currentThread());
					Thread.currentThread().wait(MAX_TIME_SECS * 1000);
				}

				waitList.multiRemove(b, Thread.currentThread());

				synchronized (locks)
				{
					if (hasXLock(b))
					{
						if (waitingTooLong(time))
						{
							throw new LockAbortException();
						}
						continue;
					}

					final int val = getLockVal(b);
					locks.put(b, val + 1);
					break;
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Error trying to obtain slock.  Throwing LockAbortException.", e);
			throw new LockAbortException();
		}
	}

	public static void unlock(Block b)
	{
		synchronized (locks)
		{
			final int val = getLockVal(b);
			if (val > 1)
			{
				locks.put(b, val - 1);
			}
			else
			{
				locks.remove(b);
				for (final Thread thread : waitList.get(b))
				{
					thread.notify();
				}
			}
		}
	}

	public static void xLock(Block b) throws LockAbortException
	{
		try
		{
			final long time = System.currentTimeMillis();
			while (true)
			{
				while (hasOtherSLocks(b) && !waitingTooLong(time))
				{
					waitList.multiPut(b, Thread.currentThread());
					Thread.currentThread().wait(MAX_TIME_SECS * 1000);
				}

				waitList.multiRemove(b, Thread.currentThread());

				synchronized (locks)
				{
					if (hasOtherSLocks(b))
					{
						if (waitingTooLong(time))
						{
							throw new LockAbortException();
						}

						continue;
					}

					locks.put(b, -1);
					break;
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Exception occurred trying to obtain xlock. LockAbortException will be thrown.", e);
			throw new LockAbortException();
		}
	}

	private static int getLockVal(Block b)
	{
		Integer ival;
		ival = locks.get(b);
		return (ival == null) ? 0 : ival.intValue();
	}

	private static boolean hasOtherSLocks(Block b)
	{
		return getLockVal(b) > 1;
	}

	private static boolean hasXLock(Block b)
	{
		return getLockVal(b) < 0;
	}

	private static boolean waitingTooLong(long time)
	{
		final long now = System.currentTimeMillis();
		return (now - time) > (MAX_TIME_SECS * 1000);
	}
}
