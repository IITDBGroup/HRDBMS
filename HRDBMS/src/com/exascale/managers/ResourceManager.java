package com.exascale.managers;

import java.io.IOException;

import com.exascale.locking.LockTable;
import com.exascale.misc.HParms;
import com.exascale.threads.HRDBMSThread;

public class ResourceManager extends HRDBMSThread
{
	protected static int percent;
	protected static long sleep;
	
	public ResourceManager()
	{
		HRDBMSWorker.logger.info("Starting initialization of the Resource Manager.");
		this.setWait(true);
		this.description = "Resource Manager";
	}
	
	public void run()
	{
		HParms hparms = HRDBMSWorker.getHParms();
		percent = Integer.parseInt(hparms.getProperty("low_mem_percent"));
		sleep = Long.parseLong(hparms.getProperty("rm_sleep_time_ms"));
		HRDBMSWorker.logger.info("Resource Manager initialization complete.");
		while (true)
		{
			if (((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) < percent)
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
	
	protected void lowMem()
	{
		//PlanCacheManager.reduce(); TODO
		System.gc();
		
		if (((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) < percent)
		{
			try
			{
				LogManager.flushAll();
			}
			catch(IOException e)
			{
				HRDBMSWorker.logger.error("Error flushing log pages in the Resource Manager.", e);
				this.terminate();
				return;
			}
			System.gc();
		}
		
		if (((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) < percent)
		{
			LockTable.blockSLocks = true;
			HRDBMSWorker.logger.warn("Low memory condition has forced the lock manager to block slocks");
			try
			{
				Thread.sleep(Long.parseLong(HRDBMSWorker.getHParms().getProperty("block_slock_time_ms")));
			}
			catch(InterruptedException e)
			{}
			LockTable.blockSLocks = false;
			HRDBMSWorker.logger.warn("The lock manager has started accepting slock requests again.");
			System.gc();
		}
		
		if (((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) < percent)
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
			HRDBMSWorker.logger.warn("Low memory condition has forced the connection manager to stop accepting connections!");
			while (true)
			{
				try
				{
					Thread.sleep(sleep);
				}
				catch(InterruptedException e)
				{}
				if (((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) < percent)
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
			
			HRDBMSWorker.logger.warn("The connection manager has resume accepting connections");
		}
	}
}
