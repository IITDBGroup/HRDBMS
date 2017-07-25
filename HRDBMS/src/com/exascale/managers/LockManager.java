package com.exascale.managers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.exceptions.LockAbortException;
import com.exascale.filesystem.Block;
import com.exascale.threads.HRDBMSThread;

public class LockManager extends HRDBMSThread
{
	private static SubLockManager[] managers;
	private static final long TIMEOUT = Long.parseLong(HRDBMSWorker.getHParms().getProperty("deadlock_check_secs")) * 1000;
	public static ConcurrentHashMap<Thread, Thread> killed = new ConcurrentHashMap<Thread, Thread>(16, 0.75f, 64 * ResourceManager.cpus);
	private static final int mLength;

	static
	{
		mLength = Runtime.getRuntime().availableProcessors() << 1;
		managers = new SubLockManager[mLength];
		int i = 0;
		while (i < mLength)
		{
			managers[i] = new SubLockManager();
			i++;
		}
	}

	public static void release(final long txnum)
	{
		int i = 0;
		while (i < mLength)
		{
			managers[i].release(txnum);
			i++;
		}
	}

	public static void sLock(final Block b, final long txnum) throws LockAbortException
	{
		final int hash = (b.hashCode2() & 0x7FFFFFFF) % mLength;
		managers[hash].sLock(b, txnum);
	}

	public static void unlockSLock(final Block b, final long txnum)
	{
		final int hash = (b.hashCode2() & 0x7FFFFFFF) % mLength;
		managers[hash].unlockSLock(b, txnum);
	}

	public static void verifyClear()
	{
		int i = 0;
		while (i < mLength)
		{
			managers[i].verifyClear();
			i++;
		}
	}

	public static void xLock(final Block b, final long txnum) throws LockAbortException
	{
		final int hash = (b.hashCode2() & 0x7FFFFFFF) % mLength;
		managers[hash].xLock(b, txnum);
	}

	@Override
	public void run()
	{
		while (true)
		{
			try
			{
				Thread.sleep(TIMEOUT);
			}
			catch (final InterruptedException e)
			{
			}

			try
			{
				final List<Vertice> starting = new ArrayList<Vertice>();
				final Map<Vertice, Vertice> vertices = new HashMap<Vertice, Vertice>();
				final Map<Thread, SubLockManager> threads2SLMs = new HashMap<Thread, SubLockManager>();

				while (true)
				{
					for (final SubLockManager manager : managers)
					{
						manager.lock.lock();
						// for all the threads that are waiting to lock blocks
						for (final Map.Entry entry : manager.waitList.entrySet())
						{
							// for each thread
							for (final Thread thread : (List<Thread>)entry.getValue())
							{
								Vertice vertice = new Vertice(manager.threads2Txs.get(thread), thread);
								threads2SLMs.put(thread, manager);
								if (!vertices.containsKey(vertice))
								{
									vertices.put(vertice, vertice);
									starting.add(vertice);
								}
								else
								{
									vertice = vertices.get(vertice);
									vertice.addThread(thread);
								}

								// if there is an xLock on that block, we know
								// what transaction we are waiting on
								final Long targetTX = manager.xBlocksToTXs.get(entry.getKey());
								if (targetTX != null)
								{
									// this xLock is what I am waiting on
									Vertice vertice2 = new Vertice(targetTX);
									if (!vertices.containsKey(vertice2))
									{
										vertices.put(vertice2, vertice2);
									}
									else
									{
										vertice2 = vertices.get(vertice2);
									}

									// create an edge from me to them
									vertice.addEdge(vertice2);
								}
								else
								{
									// otherwise I must be trying to get an
									// xLock
									// get the list of all TXs that have sLocks
									// on this block
									final Set<Long> targetTXs = manager.sBlocksToTXs.get(entry.getKey());
									for (final Long txnum : targetTXs)
									{
										Vertice vertice2 = new Vertice(txnum);
										if (!vertices.containsKey(vertice2))
										{
											vertices.put(vertice2, vertice2);
										}
										else
										{
											vertice2 = vertices.get(vertice2);
										}

										// add edges to all of those TXs
										vertice.addEdge(vertice2);
									}
								}
							}
						}
					}

					boolean doBreak = true;
					for (final Vertice vertice : starting)
					{
						final boolean cycle = checkForCycles(vertice);

						if (cycle)
						{
							vertice.removeAllEdges();
							final List<Thread> threads = vertice.getThreads();
							for (final Thread thread : threads)
							{
								killed.put(thread, thread);
								final SubLockManager manager = threads2SLMs.get(thread);
								final Long tx = manager.threads2Txs.remove(thread);
								manager.txs2Threads.remove(tx);
								final Block b = manager.inverseWaitList.remove(thread);
								final List<Thread> threads2 = manager.waitList.get(b);
								threads2.remove(thread);
								if (threads2.size() == 0)
								{
									manager.waitList.remove(b);
								}
								synchronized (thread)
								{
									thread.notify();
								}
							}

							doBreak = false;
						}
					}

					for (final SubLockManager manager : managers)
					{
						manager.lock.unlock();
					}

					if (doBreak)
					{
						break;
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.fatal("Deadlock detection thread exception", e);
				for (final SubLockManager manager : managers)
				{
					try
					{
						manager.lock.unlock();
					}
					catch (final Exception f)
					{
						HRDBMSWorker.logger.debug("", f);
					}
				}
			}
		}
	}

	private boolean checkForCycles(final Vertice vertice)
	{
		final Set<Edge> edges = new HashSet<Edge>();
		for (final Vertice dest : vertice.edges)
		{
			final Edge edge = new Edge(vertice, dest);
			edges.add(edge);
			if (checkForCycles(dest, vertice, edges))
			{
				return true;
			}
		}

		return false;
	}

	// public static void releaseUnusedXLocks(Transaction tx, Index index)
	// {
	// int i = 0;
	// while (i < mLength)
	// {
	// / managers[i].releaseUnusedXLocks(tx, index);
	// i++;
	// }
	// }

	private boolean checkForCycles(final Vertice start, final Vertice orig, final Set<Edge> edges)
	{
		for (final Vertice dest : start.edges)
		{
			if (dest.equals(orig))
			{
				return true;
			}

			final Edge edge = new Edge(start, dest);
			if (edges.contains(edge))
			{
				return false; // someone has a cycle, but its not my problem
			}
			else
			{
				edges.add(edge);
			}

			if (checkForCycles(dest, orig, edges))
			{
				return true;
			}
		}

		return false;
	}

	private static class Edge
	{
		private final Vertice source;
		private final Vertice dest;

		public Edge(final Vertice source, final Vertice dest)
		{
			this.source = source;
			this.dest = dest;
		}

		@Override
		public boolean equals(final Object r)
		{
			if (r == null)
			{
				return false;
			}
			final Edge rhs = (Edge)r;
			return source.equals(rhs.source) && dest.equals(rhs.dest);
		}

		@Override
		public int hashCode()
		{
			int hashCode = 23;
			hashCode = hashCode * 31 + source.hashCode();
			hashCode = hashCode * 31 + dest.hashCode();
			return hashCode;
		}
	}

	private static class Vertice
	{
		private final long txnum;
		private final Set<Thread> threads = new HashSet<Thread>();
		private final Set<Vertice> edges = new HashSet<Vertice>();

		public Vertice(final Long txnum)
		{
			this.txnum = txnum;
		}

		public Vertice(final Long txnum, final Thread thread)
		{
			this.txnum = txnum;
			threads.add(thread);
		}

		public void addEdge(final Vertice vertice)
		{
			edges.add(vertice);
		}

		public void addThread(final Thread thread)
		{
			threads.add(thread);
		}

		@Override
		public boolean equals(final Object r)
		{
			if (r == null)
			{
				return false;
			}
			final Vertice rhs = (Vertice)r;
			return txnum == rhs.txnum;
		}

		public List<Thread> getThreads()
		{
			return new ArrayList<Thread>(threads);
		}

		@Override
		public int hashCode()
		{
			return new Long(txnum).hashCode();
		}

		public void removeAllEdges()
		{
			edges.clear();
		}
	}
}
