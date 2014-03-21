package com.exascale.misc;

import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.optimizer.MultiOperator.AggregateThread;

public final class BufferedLinkedBlockingQueue implements Serializable
{
	public static int BLOCK_SIZE;
	private final ConcurrentHashMap<Thread, ArrayAndIndex> threadLocal = new ConcurrentHashMap<Thread, ArrayAndIndex>(64 * ResourceManager.cpus, 1.0f);
	private final ConcurrentHashMap<Thread, ArrayAndIndex> receives = new ConcurrentHashMap<Thread, ArrayAndIndex>(64 * ResourceManager.cpus, 1.0f);
	private final ArrayBlockingQueue q;

	static
	{
		HParms hparms = HRDBMSWorker.getHParms();
		BLOCK_SIZE = Integer.parseInt(hparms.getProperty("queue_block_size")); // 256
	}

	public BufferedLinkedBlockingQueue(int cap)
	{
		q = new ArrayBlockingQueue(cap / BLOCK_SIZE);
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
					final Object[] os = (Object[])q.poll();
					if (os == null)
					{
						return null;
					}
					oa = new ArrayAndIndex(os);
					receives.put(Thread.currentThread(), oa);
					break;
				}
				catch (final Exception e)
				{
				}
			}
		}

		return oa.peek(q);
	}

	public void put(Object o)
	{
		if (o == null)
		{
			Exception e = new Exception();
			HRDBMSWorker.logger.error("Null object placed on queue", e);
			System.exit(1);
		}
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
					final Object[] os = (Object[])q.take();
					// Object[] os = take2();
					oa = new ArrayAndIndex(os);
					receives.put(Thread.currentThread(), oa);
					break;
				}
				catch (final Exception e)
				{
				}
			}
		}

		return oa.take(q);
	}

	private final class ArrayAndIndex
	{
		private volatile Object[] oa;
		private int index = 0;

		public ArrayAndIndex()
		{
			oa = new Object[BLOCK_SIZE];
		}

		public ArrayAndIndex(Object[] oa)
		{
			this.oa = oa;
		}

		private void flush(ArrayBlockingQueue q)
		{
			while (true)
			{
				try
				{
					synchronized (this.oa)
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
				catch (final InterruptedException e)
				{
				}
			}
		}

		private void flushAll(ArrayBlockingQueue q, ConcurrentHashMap<Thread, ArrayAndIndex> threadLocal)
		{
			for (final ArrayAndIndex oa : threadLocal.values())
			{
				if (oa != this)
				{
					while (true)
					{
						try
						{
							synchronized (oa)
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
						catch (final InterruptedException e)
						{
						}
					}
				}
			}

			while (true)
			{
				try
				{
					synchronized (this.oa)
					{
						if (this.oa[0] != null)
						{
							q.put(this.oa);
							int i = 0;
							final Object[] temp = new Object[BLOCK_SIZE];
							temp[0] = new DataEndMarker();
							while (i < receives.size())
							{
								q.put(temp);
								i++;
							}
							this.oa = new Object[BLOCK_SIZE];
							this.index = 0;
						}
					}

					break;
				}
				catch (final InterruptedException e)
				{
				}
			}
		}

		private Object peek(ArrayBlockingQueue q)
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
						final Object[] oas = (Object[])q.poll();
						if (oas == null)
						{
							return null;
						}
						oa = oas;
						break;
					}
					catch (final Exception e)
					{
					}
				}
			}

			return oa[index++];
		}

		private void put(Object o, ArrayBlockingQueue q, ConcurrentHashMap<Thread, ArrayAndIndex> threadLocal)
		{
			synchronized (this.oa)
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

		private Object take(ArrayBlockingQueue q)
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
						// oa = (Object[])take2();
						break;
					}
					catch (final Exception e)
					{
					}
				}
			}

			return oa[index++];
		}
	}
}
