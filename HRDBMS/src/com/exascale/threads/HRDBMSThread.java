package com.exascale.threads;

import com.exascale.managers.HRDBMSWorker;

public abstract class HRDBMSThread extends Thread
{
	protected String description;
	protected boolean wait = false;
	protected long index;

	public String getDescription()
	{
		return description;
	}
	
	public void setIndex(long index)
	{
		this.index = index;
	}
	
	public boolean getWait()
	{
		return wait;
	}
	
	public void setWait(boolean wait)
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
				catch(InterruptedException e)
				{
					continue;
				}
			}
		}
	}
}
