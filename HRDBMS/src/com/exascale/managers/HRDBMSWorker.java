package com.exascale.managers;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import com.exascale.misc.HParms;
import com.exascale.optimizer.SQLParser;
import com.exascale.optimizer.SortOperator;
import com.exascale.tables.Schema;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.StartCoordsThread;
import com.exascale.threads.StartWorkersThread;
import com.exascale.threads.TempThread;
import com.exascale.threads.WarmUpThread;

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

	public static long addThread(final HRDBMSThread thread)
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

	// private static void ensureAllocation()
	// {
	// String string = hparms.getProperty("Xmx_string");
	// string = string.substring(0, string.length() - 1);
	// long gbs = Integer.parseInt(string);
	// long pretty = gbs *9/10;
	// gbs *= (1024l*1024l*1024l*9l);
	// gbs /= 10;
	// gbs /= 8;
	// long gbs2 = (long)Math.sqrt(gbs);
	// //HRDBMSWorker.logger.debug("Square root of " + gbs + " = " + gbs2);
	// int gb = (int)gbs2;
	// HRDBMSWorker.logger.debug("Attempting to allocate 8 x " + gb +" x " +
	// gb);
	// long[][] test = new long[gb][gb];
	// HRDBMSWorker.logger.debug("Allocated " + pretty + "GB");
	// }

	public static void main(final String[] args) throws Exception
	{
		logger = Logger.getLogger("LOG");
		BasicConfigurator.configure();
		fa = new RollingFileAppender(new PatternLayout("%d{ISO8601}\t%p\t%C{1}: %m%n"), "hrdbms.log", true);
		((RollingFileAppender)fa).setMaxBackupIndex(1);
		((RollingFileAppender)fa).setMaximumFileSize(2 * 1024L * 1024L * 1024L);
		fa.activateOptions();
		logger.addAppender(fa);
		logger.setLevel(Level.ALL);
		logger.info("Starting HRDBMS.");

		type = Integer.parseInt(args[0]);
		try
		{
			hparms = HParms.getHParms();
			HRDBMSWorker.getHParms().setProperty("scfc", "true");
		}
		catch (final Exception e)
		{
			logger.error("Could not load HParms", e);
			System.exit(1);
		}

		// ensureAllocation();

		try
		{
			new SQLParser();
		}
		catch (final Throwable e)
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

		long start = System.currentTimeMillis();
		if (type == TYPE_MASTER)
		{
			start -= 10000;
		}

		final Enumeration props = hparms.propertyNames();
		while (props.hasMoreElements())
		{
			final String key = (String)props.nextElement();
			HRDBMSWorker.logger.debug(key + "-> " + hparms.getProperty(key));
		}

		new FileManager();
		new Schema.CVarcharFV();
		addThread(new BufferManager(true));
		addThread(new XAManager());
		connectionThread = addThread(new ConnectionManager());
		checkpoint = new CheckpointManager(System.currentTimeMillis() - start);
		addThread(checkpoint);
		logThread = addThread(new LogManager());
		logger.info("Starting initialization of the Lock Manager.");
		addThread(new LockManager());
		logger.info("Lock Manager initialization complete.");
		resourceThread = addThread(new ResourceManager());

		// if (type == TYPE_MASTER || type == TYPE_COORD)
		// {
		// addThread(new MaintenanceManager());
		// }

		SortOperator.init();

		int i = 0;
		while (i < Runtime.getRuntime().availableProcessors())
		{
			new WarmUpThread().start();
			i++;
		}

		TempThread.initialize();
		hibernate();
	}

	public static void terminateThread(final long index)
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
				synchronized (thread)
				{
					thread.notify();
				}
			}
		}
	}

	public static void waitOnThreads(final long[] threads, final Thread waiter)
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
							synchronized (Thread.currentThread())
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

	private static void processMessage(final String msg)
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

	private static void waitListPut(final long thread, final Thread waiter)
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