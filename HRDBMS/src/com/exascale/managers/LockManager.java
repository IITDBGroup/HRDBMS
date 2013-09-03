package com.exascale;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.locks.Lock;

public class LockManager
{
	private static LockTable lockTable = new LockTable();
	private static MultiHashMap<Long, Lock> locks = new MultiHashMap<Long, Lock>();
	
	public static void sLock(Block b, long txnum) throws LockAbortException
	{
		synchronized(locks)
		{
			for (Lock l : locks.get(txnum))
			{
				if (l.block() == b)
				{
					return;
				}
			}
		}
		
		LockTable.sLock(b);
		
		synchronized(locks)
		{
			locks.multiPut(txnum, new Lock("S", b));
		}
	}
	
	public static void xLock(Block b, long txnum) throws LockAbortException
	{
		if (!hasXLock(b, txnum))
		{
			sLock(b, txnum);
			LockTable.xLock(b);
			
			synchronized(locks)
			{
				locks.multiPut(txnum, new Lock("X", b));
			}
		}
	}
	
	public static void release(long txnum)
	{
		synchronized(locks)
		{
			for (Lock lock : locks.get(txnum))
			{
				LockTable.unlock(lock.block());
				locks.multiRemove(txnum, lock);
			}
		}
	}
	
	private static boolean hasXLock(Block b, long txnum)
	{
		synchronized(locks)
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
	
	public static synchronized void unlockSLock(Block b, long txnum)
	{	
		Lock unlock = null;

		synchronized(locks)
		{
			for (Lock l : locks.get(txnum))
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
		
		synchronized(locks)
		{
			locks.multiRemove(txnum, unlock);
		}
	}
	
	public static class Lock
	{
		private String type;
		private Block b;
		
		public Lock(String type, Block b)
		{
			this.type = type;
			this.b = b;
		}
		
		public Block block()
		{
			return b;
		}
		
		public boolean equals(Object obj)
		{
			if (obj == null)
			{
				return false;
			}
			
			if (obj instanceof Lock)
			{
				Lock l = (Lock)obj;
				
				return l.type == type && l.b == b;
			}
			
			return false;
		}
	}
}
