package com.exascale.threads;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;

public abstract class ThreadPoolThread implements Runnable
{
	private volatile Future forJoin;
	private volatile boolean started = false;

	public boolean isDone()
	{
		if (!started())
		{
			return false;
		}

		return forJoin.isDone();
	}

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

	public boolean join(final int ms) throws InterruptedException
	{
		try
		{
			forJoin.get(ms, TimeUnit.MILLISECONDS);
		}
		catch (final ExecutionException e)
		{
		}
		catch (final TimeoutException e)
		{
			return false;
		}

		return true;
	}

	public void kill()
	{
		forJoin.cancel(true);
	}

	public void start()
	{
		if (!started)
		{
			forJoin = ResourceManager.pool.submit(this);
			started = true;
		}
		else
		{
			final Exception e = new Exception();
			HRDBMSWorker.logger.debug("Starting a thread that has already been started", e);
		}
	}

	public boolean started()
	{
		return forJoin != null;
	}
}
