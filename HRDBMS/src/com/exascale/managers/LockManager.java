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

public class LockManager
{
	private static SubLockManager[] managers;
	
	static
	{
		managers = new SubLockManager[Runtime.getRuntime().availableProcessors() * 2];
		int i = 0;
		while (i < managers.length)
		{
			managers[i] = new SubLockManager();
			i++;
		}
	}
	
	public static void verifyClear()
	{
		int i = 0;
		while (i < managers.length)
		{
			managers[i].verifyClear();
			i++;
		}
	}
	
	public static void release(long txnum)
	{
		int i = 0;
		while (i < managers.length)
		{
			managers[i].release(txnum);
			i++;
		}
	}

	public static void sLock(Block b, long txnum) throws LockAbortException
	{
		int hash = (b.hashCode2() & 0x7FFFFFFF) % managers.length;
		managers[hash].sLock(b, txnum);
	}

	public static void unlockSLock(Block b, long txnum)
	{
		int hash = (b.hashCode2() & 0x7FFFFFFF) % managers.length;
		managers[hash].unlockSLock(b, txnum);
	}

	public static void xLock(Block b, long txnum) throws LockAbortException
	{
		int hash = (b.hashCode2() & 0x7FFFFFFF) % managers.length;
		managers[hash].xLock(b, txnum);
	}
}
