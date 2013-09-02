package com.exascale;

import java.io.IOException;

public class ResourceManager extends HRDBMSThread
{
	private static int percent;
	private static long sleep;
	
	public ResourceManager()
	{
		this.setWait(true);
		this.description = "Resource Manager";
	}
	
	public void run()
	{
		HParms hparms = HRDBMSWorker.getHParms();
		percent = Integer.parseInt(hparms.getProperty("low_mem_percent"));
		sleep = Long.parseLong(hparms.getProperty("rm_sleep_time_ms"));
		
		while (true)
		{
			if ((Runtime.getRuntime().freeMemory() * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) < percent)
			{
				lowMem();
			}
			
			try
			{
				Thread.sleep(sleep);
			}
			catch(Exception e)
			{}
		}
	}
	
	private void lowMem()
	{
		PlanCacheManager.reduce();
		System.gc();
		if ((Runtime.getRuntime().freeMemory() * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) < percent)
		{
			try
			{
				LogManager.flushAll();
			}
			catch(IOException e)
			{
				e.printStackTrace(System.err);
				this.terminate();
			}
			System.gc();
		}
		
		if ((Runtime.getRuntime().freeMemory() * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) < percent)
		{
			LockTable.blockSLocks = true;
			System.err.println("WARNING: Low memory condition has forced the lock manager to block slocks");
			try
			{
				Thread.sleep(Long.parseLong(HRDBMSWorker.getHParms().getProperty("blocks_slock_time_ms")));
			}
			catch(InterruptedException e)
			{}
			LockTable.blockSLocks = false;
			System.err.println("The lock manager has started accepting slock requests again.");
			System.gc();
		}
		
		if ((Runtime.getRuntime().freeMemory() * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) < percent)
		{
			while (true)
			{
				try
				{
					ConnectionManager.getInputQueue().put("STOP ACCEPT");
					break;
				}
				catch(InterruptedException e)
				{
					continue;
				}
			}
			System.err.println("WARNING: Low memory condition has forced the connection manager to stop accepting connections!");
			while (true)
			{
				try
				{
					Thread.sleep(sleep);
				}
				catch(InterruptedException e)
				{}
				if ((Runtime.getRuntime().freeMemory() * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) < percent)
				{
					continue;
				}
				else
				{
					break;
				}
			}
			
			while (true)
			{
				try
				{
					ConnectionManager.getInputQueue().put("START ACCEPT");
					break;
				}
				catch(InterruptedException e)
				{
					continue;
				}
			}
			
			System.err.println("The connection manager has resume accepting connections");
		}
	}
}
