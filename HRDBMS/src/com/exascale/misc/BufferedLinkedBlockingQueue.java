package com.exascale.misc;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.optimizer.MultiOperator.AggregateThread;

public final class BufferedLinkedBlockingQueue implements Serializable
{
	public static int BLOCK_SIZE;
	private ConcurrentHashMap<Thread, ArrayAndIndex> threadLocal = new ConcurrentHashMap<Thread, ArrayAndIndex>(64 * ResourceManager.cpus, 1.0f);
	private ConcurrentHashMap<Thread, ArrayAndIndex> receives = new ConcurrentHashMap<Thread, ArrayAndIndex>(64 * ResourceManager.cpus, 1.0f);
	private volatile ArrayBlockingQueue q;
	private static Vector<ArrayBlockingQueue> free = new Vector<ArrayBlockingQueue>();
	private static int RETRY_TIME;
	private volatile boolean closed = false;

	static
	{
		HParms hparms = HRDBMSWorker.getHParms();
		BLOCK_SIZE = Integer.parseInt(hparms.getProperty("queue_block_size")); // 256
		RETRY_TIME = Integer.parseInt(hparms.getProperty("queue_flush_retry_timeout"));
	}

	public BufferedLinkedBlockingQueue(int cap)
	{
		try
		{
			q = free.remove(0);
		}
		catch(Exception e)
		{
			q = new ArrayBlockingQueue(cap / BLOCK_SIZE);
		}
	}

	public synchronized void clear()
	{
		receives.clear();
		threadLocal.clear();
		q.clear();
	}
	
	public synchronized void close()
	{
		closed = true;
		ArrayBlockingQueue temp = q;
		q = null;
		///receives.clear();
		//threadLocal.clear();
		receives = null;
		threadLocal = null;
		temp.clear();
		free.add(temp);
	}

	public Object peek()
	{
		if (closed)
		{
			return null;
		}
		
		try
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
						if (closed)
						{
							return null;
						}
						
						throw e;
					}
				}
			}

			return oa.peek();
		}
		catch(Exception e)
		{
			if (closed)
			{
				return null;
			}
			
			throw e;
		}
	}

	public void put(Object o)
	{
		if (closed)
		{
			return;
		}
		try
		{
			if (o == null)
			{
				Exception e = new Exception("Null object placed on queue");
				HRDBMSWorker.logger.error("Null object placed on queue", e);
				return;
			}
		
			if (o instanceof ArrayList && ((ArrayList)o).size() == 0)
			{
				HRDBMSWorker.logger.debug("ArrayList of size zero was placed on queue");
				return;
			}
		
			ArrayAndIndex oa = threadLocal.get(Thread.currentThread());
			if (oa == null)
			{
				oa = new ArrayAndIndex();
				threadLocal.put(Thread.currentThread(), oa);
			}

			oa.put(o, threadLocal);
		}
		catch(Exception e)
		{
			if (closed)
			{
				return;
			}
			
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
	}

	public Object take() throws Exception
	{
		try
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

			Object retval = oa.take();
			if (retval == null)
			{
				throw new Exception("take() is returning a null value");
			}
		
			return retval;
		}
		catch(Exception e)
		{
			if (closed)
			{
				return null;
			}
			
			throw e;
		}
	}

	private final class ArrayAndIndex
	{
		private volatile Object[] oa;
		private volatile int index = 0;

		public ArrayAndIndex()
		{
			oa = new Object[BLOCK_SIZE];
		}

		public ArrayAndIndex(Object[] oa)
		{
			this.oa = oa;
		}

		private synchronized void flush()
		{
			while (true)
			{
				try
				{
					if (this.oa[0] != null)
					{
						q.put(this.oa);
						this.oa = new Object[BLOCK_SIZE];
						this.index = 0;
					}

					break;
				}
				catch (final Exception e)
				{
				}
			}
		}

		private boolean flushAll(ConcurrentHashMap<Thread, ArrayAndIndex> threadLocal, Object o)
		{
			synchronized(BufferedLinkedBlockingQueue.this)
			{
				synchronized(this)
				{
					for (final ArrayAndIndex oa : threadLocal.values())
					{
						if (oa != this)
						{
							while (true)
							{
								synchronized(oa)
								{
									if (oa.oa[0] != null)
									{
										if (!q.offer(oa.oa))
										{
											if (!(oa.oa[0] instanceof DataEndMarker))
											{
												HRDBMSWorker.logger.debug("FlushAll failed because we could not flush someone else's data");
												return false;
											}
										}
										oa.oa = new Object[BLOCK_SIZE];
										oa.index = 0;
									}
									break;
								}
							}
						}
					}

					while (true)
					{
						if (this.oa[0] != null)
						{
							if (!q.offer(this.oa))
							{
								if (!(this.oa[0] instanceof DataEndMarker))
								{
									HRDBMSWorker.logger.debug("FlushAll failed because I could not flush my deata");
									return false;
								}
							}
					
							this.oa = new Object[BLOCK_SIZE];
							this.index = 0;
						}
					
						Object[] temp = new Object[BLOCK_SIZE];
						temp[0] = o;
						if (!q.offer(temp))
						{
							if (!(temp[0] instanceof DataEndMarker))
							{
								HRDBMSWorker.logger.debug("FlushAll failed because I couldn't write my data that initiated the flushAll and it's not a DataEndMarker");
								HRDBMSWorker.logger.debug("It is " + temp[0]);
								return false;
							}
							
							if (!(((Object[])q.peek())[0] instanceof DataEndMarker))
							{
								HRDBMSWorker.logger.debug("FlushAll failed because I have a DataEndMarker to write and the head of the queue is " + ((Object[])q.peek())[0]);
								return false;
							}
						}
					
						int i = 0;
						int safetyNet = receives.size();
						while (i < safetyNet)
						{
							temp = new Object[BLOCK_SIZE];
							temp[0] = new DataEndMarker();
							q.offer(temp);
							i++;
						}
				
						break;
					}
			
					return true;
				}
			}
		}

		private synchronized Object peek()
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

		private void put(Object o, ConcurrentHashMap<Thread, ArrayAndIndex> threadLocal)
		{
			if (o instanceof DataEndMarker || o instanceof Exception)
			{
				while (true)
				{
					boolean ok = true;
					ok = flushAll(threadLocal, o);
					
					if (ok)
					{
						break;
					}
					
					try
					{
						Thread.sleep(RETRY_TIME);
					}
					catch(Exception e)
					{}
				}
			}
			else if (o instanceof AggregateThread && ((AggregateThread)o).isEnd())
			{
				while (true)
				{
					boolean ok = true;
					ok = flushAll(threadLocal, o);
					
					if (ok)
					{
						break;
					}
					
					try
					{
						Thread.sleep(RETRY_TIME);
					}
					catch(Exception e)
					{}
				}
			}
			else
			{
				synchronized(this)
				{
					oa[index++] = o;
				}
				
				if (index == BLOCK_SIZE)
				{
					flush();
				}
			}
		}

		private synchronized Object take() throws Exception
		{
			if (index < BLOCK_SIZE && oa[index] != null)
			{
				Object retval = oa[index++];
				if (retval == null)
				{
					throw new Exception("OA.take() returning null value first path");
				}
				
				return retval;
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
						HRDBMSWorker.logger.debug("", e);
					}
				}
			}

			Object retval = oa[index++];
			if (retval == null)
			{
				throw new Exception("OA.take() returning null value second path");
			}
			
			return retval;
		}
	}
}
