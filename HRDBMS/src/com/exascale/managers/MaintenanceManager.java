package com.exascale.managers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import com.exascale.optimizer.MetaData;
import com.exascale.tasks.DeleteFilesTask;
import com.exascale.tasks.InitReorgTask;
import com.exascale.tasks.InitRunstatsTask;
import com.exascale.tasks.Task;
import com.exascale.threads.HRDBMSThread;

public class MaintenanceManager extends HRDBMSThread
{
	private MetaData meta = new MetaData();
	private static PriorityBlockingQueue<Task> tasks = new PriorityBlockingQueue<Task>();
	public static ConcurrentHashMap<String, String> failed = new ConcurrentHashMap<String, String>();
	public static ConcurrentHashMap<String, String> reorgFailed = new ConcurrentHashMap<String, String>();
	private transient static long busyTill = -1;
	
	public MaintenanceManager() throws Exception
	{
		try
		{
			Class.forName("com.exascale.client.HRDBMSDriver");
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.error("Unable to load HRDBMS driver!");
			System.exit(1);
		}
		
		long now = System.currentTimeMillis();
		schedule(new InitRunstatsTask(), now + 24 * 60 * 60 * 1000);
		schedule(new InitReorgTask(), now + 24 * 60 * 60 * 1000);
		
		if (meta.myCoordNum() * -1 - 2 < 3)
		{
			schedule(new DeleteFilesTask(), now + Integer.parseInt(HRDBMSWorker.getHParms().getProperty("old_file_cleanup_target_days")) * 24 * 60 * 60 * 1000);
		}
	}
	
	public static synchronized void schedule(Task task, long executionTime)
	{
		task.setExecuteTime(executionTime);
		task.setEstimatedTime(0);
		tasks.add(task);
	}
	
	public static void schedule(Task task, long executionTime, long estimatedRunTime)
	{
		schedule(task, -1, estimatedRunTime, executionTime + estimatedRunTime);
		//task.setExecuteTime(executionTime);
		//task.setEstimatedTime(estimatedRunTime);
		//tasks.add(task);
	}
	
	public static synchronized void schedule(Task task, long executionTime, long estimatedRunTime, long targetCompletionTime)
	{
		task.setEstimatedTime(estimatedRunTime);
		//figure out how to schedule it
		//find a free spot
		//compress things to make a free spot
		//next depth
		ArrayList<ArrayList<Task>> depths = buildDepths();
		long time = -1;
		int i = 0;
		while (i < depths.size())
		{
			Free free = largeCloseFree(targetCompletionTime - estimatedRunTime, targetCompletionTime, depths.get(i));
			if (free != null)
			{
				time = free.center(estimatedRunTime);
				task.setExecuteTime(time);
				tasks.add(task);
				return;
			}
		
			long total = totalPriorFree(depths.get(i), targetCompletionTime);
			if (total >= estimatedRunTime)
			{
				makeSpace(depths.get(i), targetCompletionTime - estimatedRunTime, targetCompletionTime);
				time = targetCompletionTime - estimatedRunTime;
				task.setExecuteTime(time);
				tasks.add(task);
				return;
			}
		
			i++;
		}
		
		time = targetCompletionTime - estimatedRunTime;
		task.setExecuteTime(time);
		tasks.add(task);
		return;
	}
	
	private static void makeSpace(ArrayList<Task> layer, long start, long end)
	{
		int i = 0;
		for (Task task : layer)
		{
			if (task.executeTime() >= end)
			{
				break;
			}
		}
		
		i--;
		int last = i;
		while (i >= 0)
		{
			Task task = layer.get(i);
			if (i != 0)
			{
				Task prev = layer.get(i-1);
				task.setExecuteTime(prev.executeTime() + prev.getEstimate());
			}
			else
			{
				task.setExecuteTime(Math.max(System.currentTimeMillis(), busyTill));
			}
			
			int j = i + 1;
			while (j <= last)
			{
				task = layer.get(j);
				Task prev = layer.get(j - 1);
				task.setExecuteTime(prev.executeTime() + prev.getEstimate());
				j++;
			}
			
			i--;
			
			task = layer.get(last);
			if ((task.executeTime() + task.getEstimate()) <= start)
			{
				break;
			}
		}
		
		for (Task task : layer)
		{
			tasks.remove(task);
			tasks.add(task);
		}
	}
	
	private static long totalPriorFree(ArrayList<Task> layer, long endTime)
	{
		int i = 0;
		long retval = 0;
		Task prev = null;
		for (Task task : layer)
		{
			if (i == 0)
			{
				if (task.executeTime() <= endTime)
				{
					if (task.executeTime() > busyTill)
					{
						retval += (task.executeTime() - Math.max(System.currentTimeMillis(), busyTill));
					}
				
					prev = task;
				}
				else
				{
					retval += (endTime - Math.max(System.currentTimeMillis(), busyTill));
					break;
				}
			}
			else
			{
				if (task.executeTime() <= endTime)
				{
					retval += (task.executeTime() - (prev.executeTime() + prev.getEstimate()));
					prev = task;
				}
				else
				{
					retval += (endTime - (prev.executeTime() + prev.getEstimate()));
					break;
				}
			}
			
			i++;
		}
		
		if (i == layer.size())
		{
			Task last = layer.get(i - 1);
			long remaining = endTime - (last.executeTime() + last.getEstimate());
			if (remaining > 0)
			{
				retval += remaining;
			}
		}
		
		return retval;
	}
	
	private static Free largeCloseFree(long startRequest, long endRequest, ArrayList<Task> layer)
	{
		//start is inside a task or outside
		int i = 0;
		for (Task task : layer)
		{
			if (startRequest >= task.executeTime() && startRequest <= (task.executeTime() + task.getEstimate()))
			{
				//start is in here
				if (i == 0)
				{
					return new Free(System.currentTimeMillis(), System.currentTimeMillis() + (endRequest - startRequest));
				}
				else
				{
					while (i > 0)
					{
						Task prev = layer.get(i - 1);
						long end = task.executeTime();
						long start = prev.executeTime() + prev.getEstimate();
						if ((end - start) >= (endRequest - startRequest))
						{
							return new Free(start, end);
						}
					
						i--;
						task = layer.get(i);
					}
					
					return null;
				}
			}
			else if (startRequest < task.executeTime())
			{
				//start is not in a task
				if (i == 0)
				{
					long end = Math.min(endRequest, task.executeTime());
					long start = end - (endRequest - startRequest);
					
					if ((busyTill == -1 || start >= busyTill) && start >= System.currentTimeMillis())
					{
						return new Free(start, end);
					}
					else
					{
						return new Free(System.currentTimeMillis(), System.currentTimeMillis() + (endRequest - startRequest));
					}
				}
				else
				{
					long end = Math.min(endRequest, task.executeTime());
					Task prev = layer.get(i - 1);
					long start = prev.executeTime() + prev.getEstimate();
					if ((end - start) >= (endRequest - startRequest))
					{
						return new Free(start, end);
					}
					
					i--;
					task = layer.get(i);
					while (i > 0)
					{
						prev = layer.get(i - 1);
						end = task.executeTime();
						start = prev.executeTime() + prev.getEstimate();
						if ((end - start) >= (endRequest - startRequest))
						{
							return new Free(start, end);
						}
					
						i--;
						task = layer.get(i);
					}
					
					return null;
				}
			}
			
			i++;
		}
		
		//start is after end of last task
		return new Free(startRequest, endRequest);
	}
	
	private static class Free
	{
		private long start;
		private long end;
		
		public Free(long start, long end)
		{
			this.start = start;
			this.end = end;
		}
		
		public long center(long estimate)
		{
			long free = end - start - estimate;
			return start + free / 2;
		}
	}
	
	private static ArrayList<ArrayList<Task>> buildDepths()
	{
		ArrayList<ArrayList<Task>> retval = new ArrayList<ArrayList<Task>>();
		ArrayList<Task> level = new ArrayList<Task>();
		ArrayList<Task> queued = new ArrayList<Task>();
		ArrayList<Task> list = new ArrayList<Task>(tasks);
		while (true)
		{
			Collections.sort(list, new ScheduleComparator());
			for (Task task : list)
			{
				if (level.size() == 0)
				{
					level.add(task);
				}
				else
				{
					Task prev = level.get(level.size() - 1);
					if (task.executeTime() >= (prev.executeTime() + prev.getEstimate()))
					{
						level.add(task);
					}
					else
					{
						queued.add(task);
					}
				}
			}
		
			retval.add(level);
			if (queued.size() > 0)
			{
				level = new ArrayList<Task>();
				list = queued;
				queued = new ArrayList<Task>();
			}
			else
			{
				return retval;
			}
		}
	}
	
	private static class ScheduleComparator implements Comparator<Task>
	{
		@Override
		public int compare(Task lhs, Task rhs)
		{
			long lhsEnd = lhs.executeTime() + lhs.getEstimate();
			long rhsEnd = rhs.executeTime() + rhs.getEstimate();
			
			if (lhsEnd < rhsEnd)
			{
				return -1;
			}
			else if (lhsEnd > rhsEnd)
			{
				return 1;
			}
			else
			{
				return 0;
			}
		}
		
	}
	
	public void run()
	{
		while (true)
		{
			try
			{
				Task task = null;
				while (true)
				{
					task = tasks.peek();
					long sleep = Long.MAX_VALUE;
					if (task != null)
					{
						sleep = task.executeTime() - System.currentTimeMillis();
					}
					
					if (sleep > 0)
					{
						Thread.sleep(Math.min(sleep, 60000));
					}
					else
					{
						break;
					}
				}
				
				synchronized(MaintenanceManager.class)
				{
					tasks.remove(task);
					long newBusy = System.currentTimeMillis() + task.getEstimate();
					if (newBusy > busyTill)
					{
						busyTill = newBusy;
					}
				
					task.run();
				}
			}
			catch(InterruptedException e)
			{}
		}
	}
}
