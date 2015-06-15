package com.exascale.threads;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import com.exascale.managers.ResourceManager;

public abstract class ThreadPoolThread implements Runnable
{
	private volatile Future forJoin;

	public void join() throws InterruptedException
	{
		try
		{
			forJoin.get();
		}
		catch (final ExecutionException e)
		{
		}
	}
	
	public boolean started()
	{
		return forJoin != null;
	}

	public void kill()
	{
		forJoin.cancel(true);
	}

	public void start()
	{
		forJoin = ResourceManager.pool.submit(this);
	}
}
