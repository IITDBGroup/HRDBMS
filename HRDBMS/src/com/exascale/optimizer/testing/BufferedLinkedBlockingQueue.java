package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.exascale.optimizer.testing.MultiOperator.AggregateThread;

public class BufferedLinkedBlockingQueue  implements Serializable
{
	protected final static int BLOCK_SIZE = 256;
	protected ConcurrentHashMap<Thread, ArrayAndIndex> threadLocal = new ConcurrentHashMap<Thread, ArrayAndIndex>(64 * ResourceManager.cpus);
	protected ConcurrentHashMap<Thread, ArrayAndIndex> receives = new ConcurrentHashMap<Thread, ArrayAndIndex>(64 * ResourceManager.cpus);
	protected LinkedBlockingQueue q;
	
	public BufferedLinkedBlockingQueue(int cap)
	{
		q = new LinkedBlockingQueue(cap / BLOCK_SIZE);
	}
	
	public void clear()
	{
		receives.clear();
		threadLocal.clear();
		q.clear();
	}
	
	public Object peek()
	{
		ArrayAndIndex oa = receives.get(Thread.currentThread());
		if (oa == null)
		{
			while (true)
			{
				try
				{
					Object[] os = (Object[])q.poll();
					if (os == null)
					{
						return null;
					}
					oa = new ArrayAndIndex(os);
					receives.put(Thread.currentThread(), oa);
					break;
				}
				catch(Exception e)
				{}
			}
		}
		
		return oa.peek(q);
	}
	
	public void put(Object o)
	{
		ArrayAndIndex oa = threadLocal.get(Thread.currentThread());
		if (oa == null)
		{
			oa = new ArrayAndIndex();
			threadLocal.put(Thread.currentThread(), oa);
		}
		
		oa.put(o, q, threadLocal);
	}
	
	public Object take()
	{
		ArrayAndIndex oa = receives.get(Thread.currentThread());
		if (oa == null)
		{
			while (true)
			{
				try
				{
					Object[] os = (Object[])q.take();
					//Object[] os = take2();
					oa = new ArrayAndIndex(os);
					receives.put(Thread.currentThread(), oa);
					break;
				}
				catch(Exception e)
				{}
			}
		}
		
		return oa.take(q);
	}
	
	private static class ArrayAndIndex
	{
		protected volatile Object[] oa;
		protected int index = 0;
		
		public ArrayAndIndex()
		{
			oa = new Object[BLOCK_SIZE];
		}
		
		public ArrayAndIndex(Object[] oa)
		{
			this.oa = oa;
		}
		
		private void put(Object o, LinkedBlockingQueue q, ConcurrentHashMap<Thread, ArrayAndIndex> threadLocal)
		{
			synchronized(this.oa)
			{
				oa[index++] = o;
			}
			
			if (o instanceof DataEndMarker)
			{
				flushAll(q, threadLocal);
			}
			else if (o instanceof AggregateThread && ((AggregateThread)o).isEnd())
			{
				flushAll(q, threadLocal);
			}
			else if (index == BLOCK_SIZE)
			{
				flush(q);
			}
		}
		
		private Object peek(LinkedBlockingQueue q)
		{
			if (index < BLOCK_SIZE && oa[index] != null)
			{
				return oa[index];
			}
			
			index = 0;
			oa[index] = null;
			while (oa[index] == null)
			{
				while (true)
				{
					try
					{
						Object[] oas = (Object[])q.poll();
						if (oas == null)
						{
							return null;
						}
						oa = oas;
						break;
					}
					catch(Exception e)
					{}
				}
			}
			
			return oa[index++];
		}
		
		private Object take(LinkedBlockingQueue q)
		{
			if (index < BLOCK_SIZE && oa[index] != null)
			{
				return oa[index++];
			}
		
			index = 0;
			oa[index] = null;
			while (oa[index] == null)
			{
				while (true)
				{
					try
					{
						oa = (Object[])q.take();
						//oa = (Object[])take2();
						break;
					}
					catch(Exception e)
					{}
				}
			}
			
			return oa[index++];
		}
		
		private void flush(LinkedBlockingQueue q)
		{
			while (true)
			{
				try
				{
					synchronized(this.oa)
					{
						if (this.oa[0] != null)
						{
							q.put(this.oa);
							this.oa = new Object[BLOCK_SIZE];
							this.index = 0;
						}
					}
					
					break;
				}
				catch(InterruptedException e)
				{}
			}
		}
		
		private void flushAll(LinkedBlockingQueue q, ConcurrentHashMap<Thread, ArrayAndIndex> threadLocal)
		{
			for (ArrayAndIndex oa : threadLocal.values())
			{
				if (oa != this)
				{
					while (true)
					{
						try
						{
							synchronized(oa)
							{
								if (oa.oa[0] != null)
								{
									q.put(oa.oa);
									oa.oa = new Object[BLOCK_SIZE];
									oa.index = 0;
								}
							}
							
							break;
						}
						catch(InterruptedException e)
						{}
					}
				}
			}
			
			while (true)
			{
				try
				{
					synchronized(this.oa)
					{
						if (this.oa[0] != null)
						{
							q.put(this.oa);
							this.oa = new Object[BLOCK_SIZE];
							this.index = 0;
						}
					}
					
					break;
				}
				catch(InterruptedException e)
				{}
			}
		}
	}
}
