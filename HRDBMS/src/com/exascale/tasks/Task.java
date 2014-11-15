package com.exascale.tasks;

public abstract class Task implements Comparable<Task>
{
	private long time;
	private long estimate;
	
	public long executeTime()
	{
		return time;
	}
	
	public void setExecuteTime(long time)
	{
		this.time = time;
	}
	
	public void setEstimatedTime(long estimate)
	{
		this.estimate = estimate;
	}
	
	public long getEstimate()
	{
		return estimate;
	}
	
	public abstract void run();
	
	public int compareTo(Task rhs)
	{
		if (time < rhs.time)
		{
			return -1;
		}
		else if (time > rhs.time)
		{
			return 1;
		}
		else
		{
			return 0;
		}
	}
}
