package com.exascale.managers;

import java.util.Vector;
import com.exascale.exceptions.LockAbortException;
import com.exascale.filesystem.Block;
import com.exascale.locking.LockTable;
import com.exascale.misc.MultiHashMap;

public class LockManager
{
	private static LockTable lockTable = new LockTable();

	private static MultiHashMap<Long, Lock> locks = new MultiHashMap<Long, Lock>();

	public static void release(long txnum)
	{
		synchronized (locks)
		{
			Vector<Lock> clone = (Vector<Lock>)locks.get(txnum).clone();
			for (final Lock lock : clone)
			{
				LockTable.unlock(lock.block());
				locks.multiRemove(txnum, lock);
			}
		}
	}

	public static void sLock(Block b, long txnum) throws LockAbortException
	{
		synchronized (locks)
		{
			for (final Lock l : locks.get(txnum))
			{
				if (l.block().equals(b))
				{
					return;
				}
			}
		}

		LockTable.sLock(b);

		synchronized (locks)
		{
			locks.multiPut(txnum, new Lock("S", b));
		}
	}

	public static synchronized void unlockSLock(Block b, long txnum)
	{
		Lock unlock = null;

		synchronized (locks)
		{
			for (final Lock l : locks.get(txnum))
			{
				if (l.block() == b && l.type == "S")
				{
					unlock = l;
				}

				if (l.block() == b && l.type == "X")
				{
					return;
				}
			}
		}

		LockTable.unlock(b);

		synchronized (locks)
		{
			locks.multiRemove(txnum, unlock);
		}
	}

	public static void xLock(Block b, long txnum) throws LockAbortException
	{
		if (!hasXLock(b, txnum))
		{
			sLock(b, txnum);
			LockTable.xLock(b);

			synchronized (locks)
			{
				locks.multiPut(txnum, new Lock("X", b));
			}
		}
	}

	private static boolean hasXLock(Block b, long txnum)
	{
		synchronized (locks)
		{
			if (!locks.multiContains(txnum, new Lock("X", b)))
			{
				return false;
			}
			else
			{
				return true;
			}
		}
	}

	public static class Lock
	{
		private final String type;
		private final Block b;

		public Lock(String type, Block b)
		{
			this.type = type;
			this.b = b;
		}

		public Block block()
		{
			return b;
		}

		@Override
		public boolean equals(Object obj)
		{
			if (obj == null)
			{
				return false;
			}

			if (obj instanceof Lock)
			{
				final Lock l = (Lock)obj;

				return l.type == type && l.b == b;
			}

			return false;
		}
	}
}
