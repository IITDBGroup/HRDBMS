package com.exascale.managers;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import com.exascale.misc.HParms;
import com.exascale.optimizer.SQLParser;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.StartCoordsThread;
import com.exascale.threads.StartWorkersThread;

public class HRDBMSWorker
{
	private static HParms hparms; // configurable parameters
	public static final int TYPE_MASTER = 0, TYPE_COORD = 1, TYPE_WORKER = 2; // master
																				// is
																				// the
																				// first
																				// coord
	public static int type; // my type
	private static ConcurrentHashMap<Long, HRDBMSThread> threadList = new ConcurrentHashMap<Long, HRDBMSThread>();
	private static long connectionThread;
	private static long logThread;
	private static long resourceThread;
	private static BlockingQueue<String> in = new LinkedBlockingQueue<String>(); // message
																					// queue
	private static AtomicLong THREAD_NUMBER = new AtomicLong(0);
	private static ConcurrentHashMap<Long, ArrayList<Thread>> waitList = new ConcurrentHashMap<Long, ArrayList<Thread>>();
	public static org.apache.log4j.Logger logger;
	public static volatile CheckpointManager checkpoint;
	private static FileAppender fa;

	public static long addThread(HRDBMSThread thread)
	{
		long retval;
		retval = THREAD_NUMBER.getAndIncrement();
		thread.setIndex(retval);
		threadList.put(retval, thread);
		thread.start();
		return retval;
	}

	public static long getConnectionThread()
	{
		return connectionThread;
	}

	// get configurable properties
	public static HParms getHParms()
	{
		return hparms;
	}

	public static BlockingQueue<String> getInputQueue()
	{
		return in;
	}

	public static long getLogThread()
	{
		return logThread;
	}

	public static long getResourceThread()
	{
		return resourceThread;
	}

	public static ConcurrentHashMap<Long, HRDBMSThread> getThreadList()
	{
		return threadList;
	}

	public static void main(String[] args) throws Exception
	{
		logger = Logger.getLogger("LOG");
		BasicConfigurator.configure();
		fa = new FileAppender(new PatternLayout("%d{ISO8601}\t%p\t%C{1}: %m%n"), "hrdbms.log"); 
		fa.activateOptions();
		logger.addAppender(fa);
		logger.setLevel(Level.ALL);
		logger.info("Starting HRDBMS.");

		type = Integer.parseInt(args[0]);
		try
		{
			hparms = HParms.getHParms();
		}
		catch (final Exception e)
		{
			logger.error("Could not load HParms", e);
			System.exit(1);
		}
		
		try
		{
			SQLParser bug = new SQLParser();
		}
		catch(Throwable e)
		{
			HRDBMSWorker.logger.fatal("Can't load SQLParser class", e);
			System.exit(1);
		}

		/*
		 * File f1 = new File("hrdbms.log"); f1.createNewFile(); PrintStream ps
		 * = new PrintStream(new FileOutputStream(f1), false);
		 * System.setOut(ps);
		 */

		if (type == TYPE_MASTER)
		{
			final long[] threads = new long[2];
			threads[0] = addThread(startWorkers());
			threads[1] = addThread(startCoordinators());
			waitOnThreads(threads, Thread.currentThread());
			logger.info("Done starting all nodes.");
			Thread.sleep(10000);
		}

		if (type == TYPE_MASTER || type == TYPE_COORD)
		{
			addThread(new MetaDataManager());
		}

		new FileManager();
		new BufferManager(true);
		addThread(new XAManager());
		connectionThread = addThread(new ConnectionManager());
		checkpoint = new CheckpointManager();
		addThread(checkpoint);
		logThread = addThread(new LogManager());
		logger.info("Starting initialization of the Lock Manager.");
		new LockManager();
		logger.info("Lock Manager initialization complete.");
		resourceThread = addThread(new ResourceManager());
		hibernate();
	}

	public static void waitOnThreads(long[] threads, Thread waiter)
	{
		boolean done = false;
		while (!done)
		{
			done = true;
			for (final long thread : threads)
			{
				if (threadList.containsKey(thread))
				{
					boolean needsWait = false;
					synchronized (threadList)
					{
						if (threadList.containsKey(thread))
						{
							needsWait = true;
							done = false;
							// if thread in list, add to wait list that this
							// thread is waiting on it
							waitListPut(thread, waiter);
						}
					}

					if (needsWait)
					{
						try
						{
							synchronized(Thread.currentThread())
							{
								Thread.currentThread().wait();
								break;
							}
						}
						catch (final InterruptedException e)
						{
						}
					}
				}
			}
		}
	}

	private static void hibernate()
	{
		String msg;
		logger.info("Main thread is about to hibernate.");
		while (true)
		{
			try
			{
				msg = in.take();
				processMessage(msg);
			}
			catch (final InterruptedException e)
			{
				continue;
			}
		}
	}

	private static void processMessage(String msg)
	{
		if (msg.equals("SHUTDOWN"))
		{
			// TODO: Graceful shutdown
		}

		if (msg.startsWith("TERMINATE THREAD "))
		{
			terminateThread(Integer.parseInt(msg.substring(17)));
		}
	}

	private static StartCoordsThread startCoordinators()
	{
		return new StartCoordsThread();
	}

	private static StartWorkersThread startWorkers()
	{
		return new StartWorkersThread();
	}

	public static void terminateThread(long index)
	{
		// if anyone is waiting on this thread, wait for Thread.getState() ==
		// WAITING and notify them
		if (waitList.containsKey(index))
		{
			final ArrayList<Thread> threads = waitList.get(index);
			for (final Thread thread : threads)
			{
				while (thread.getState() != Thread.State.WAITING)
				{
				}
				synchronized(thread)
				{
					thread.notify();
				}
			}
		}
	}

	private static void waitListPut(long thread, Thread waiter)
	{
		synchronized (waitList)
		{
			if (!waitList.containsKey(thread))
			{
				final ArrayList<Thread> temp = new ArrayList<Thread>();
				temp.add(waiter);
				waitList.put(thread, temp);
			}
			else
			{
				final ArrayList<Thread> temp = waitList.get(thread);
				temp.add(waiter);
			}
		}
	}
}