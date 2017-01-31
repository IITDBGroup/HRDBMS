package com.exascale.threads;

import com.exascale.managers.HRDBMSWorker;

public abstract class HRDBMSThread extends ThreadPoolThread
{
	protected String description;
	private boolean wait = false;
	protected long index;

	public String getDescription()
	{
		return description;
	}

	public boolean getWait()
	{
		return wait;
	}

	public void setIndex(final long index)
	{
		this.index = index;
	}

	public void setWait(final boolean wait)
	{
		this.wait = wait;
	}

	protected void terminate()
	{
		HRDBMSWorker.getThreadList().remove(index);
		if (wait)
		{
			while (true)
			{
				try
				{
					HRDBMSWorker.getInputQueue().put("TERMINATE THREAD " + index);
					break;
				}
				catch (final InterruptedException e)
				{
					continue;
				}
			}
		}
	}
}
