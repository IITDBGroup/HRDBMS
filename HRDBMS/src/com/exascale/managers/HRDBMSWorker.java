package com.exascale.managers;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import com.exascale.misc.HParms;
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
	private static long bufferThread;
	private static long logThread;
	private static long resourceThread;
	private static long checkpointThread;
	private static long metaDataThread;
	private static BlockingQueue<String> in = new LinkedBlockingQueue<String>(); // message
																					// queue
	private static AtomicLong THREAD_NUMBER = new AtomicLong(0);
	private static ConcurrentHashMap<Long, ArrayList<Thread>> waitList = new ConcurrentHashMap<Long, ArrayList<Thread>>();
	public static org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger("Log");

	public static long addThread(HRDBMSThread thread)
	{
		long retval;
		retval = THREAD_NUMBER.getAndIncrement();
		thread.setIndex(retval);
		threadList.put(retval, thread);
		thread.start();
		return retval;
	}

	public static long getBufferThread()
	{
		return bufferThread;
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
		final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger("Log");
		final org.apache.logging.log4j.core.Logger coreLogger = (org.apache.logging.log4j.core.Logger)logger;
		final org.apache.logging.log4j.core.LoggerContext context = coreLogger.getContext();
		final org.apache.logging.log4j.core.config.BaseConfiguration conf = (org.apache.logging.log4j.core.config.BaseConfiguration)context.getConfiguration();
		final FileAppender fa = FileAppender.createAppender("hrdbms.log", "false", "false", "FileLogger", "true", "false", "true", PatternLayout.createLayout("%d{ISO8601}\t%p\t%C{1}: %m%ex%n", conf, null, "UTF-8", "no"), null, "false", null, conf);
		fa.start();
		coreLogger.addAppender(fa);
		coreLogger.setLevel(Level.ALL);
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
		}

		if (type == TYPE_MASTER || type == TYPE_COORD)
		{
			metaDataThread = addThread(new MetaDataManager());
		}

		new FileManager();
		bufferThread = addThread(new BufferManager(true));
		logThread = addThread(new LogManager());
		logger.info("Starting initialization of the Lock Manager.");
		new LockManager();
		logger.info("Lock Manager initialization complete.");
		resourceThread = addThread(new ResourceManager());
		connectionThread = addThread(new ConnectionManager());
		checkpointThread = addThread(new CheckpointManager());
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
							Thread.currentThread().wait();
							break;
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

	private static void terminateThread(long index)
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
				thread.notify();
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