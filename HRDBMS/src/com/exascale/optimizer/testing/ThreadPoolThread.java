package com.exascale.optimizer.testing;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class ThreadPoolThread implements Runnable
{
	protected Future forJoin;
	
	public void start()
	{
		forJoin = ResourceManager.pool.submit(this);
	}
	
	public void join() throws InterruptedException
	{
		try
		{
			forJoin.get();
		}
		catch(ExecutionException e)
		{}
	}
}
