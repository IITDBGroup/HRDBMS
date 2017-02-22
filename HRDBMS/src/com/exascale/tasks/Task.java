package com.exascale.tasks;

public abstract class Task implements Comparable<Task>
{
	private long time;
	private long estimate;

	@Override
	public int compareTo(final Task rhs)
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

	public long executeTime()
	{
		return time;
	}

	public long getEstimate()
	{
		return estimate;
	}

	public abstract void run();

	public void setEstimatedTime(final long estimate)
	{
		this.estimate = estimate;
	}

	public void setExecuteTime(final long time)
	{
		this.time = time;
	}
}
